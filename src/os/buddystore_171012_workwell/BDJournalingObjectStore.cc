// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "BDJournalingObjectStore.h"

#include "common/errno.h"
#include "common/debug.h"

#define dout_context cct
#define dout_subsys ceph_subsys_journal
#undef dout_prefix
#define dout_prefix *_dout << "journal "



void BDJournalingObjectStore::journal_start()
{
  dout(10) << "journal_start" << dendl;
  finisher.start();
}

void BDJournalingObjectStore::journal_stop()
{
  dout(10) << "journal_stop" << dendl;
  finisher.wait_for_empty();
  finisher.stop();
}

// A journal_replay() makes journal writeable, this closes that out.
void BDJournalingObjectStore::journal_write_close()
{
  if (journal) {
    journal->close();
    delete journal;
    journal = 0;
  }
  apply_manager.reset();
}

int BDJournalingObjectStore::journal_replay(uint64_t fs_op_seq)
{
  dout(10) << "journal_replay fs op_seq " << fs_op_seq << dendl;

#if 0
  if (cct->_conf->journal_replay_from) {
    dout(0) << "journal_replay forcing replay from "
	    << cct->_conf->journal_replay_from
	    << " instead of " << fs_op_seq << dendl;
    // the previous op is the last one committed
    fs_op_seq = cct->_conf->journal_replay_from - 1;
  }
#endif
  uint64_t op_seq = fs_op_seq;
  apply_manager.init_seq(fs_op_seq);

  if (!journal) {
    submit_manager.set_op_seq(op_seq);
    return 0;
  }

  int err = journal->open(op_seq);
  if (err < 0) {
    dout(3) << "journal_replay open failed with "
	    << cpp_strerror(err) << dendl;
    delete journal;
    journal = 0;
    return err;
  }

  replaying = true;

  int count = 0;
  while (1) {
    bufferlist bl;
    uint64_t seq = op_seq + 1;
    if (!journal->read_entry(bl, seq)) {
      dout(3) << "journal_replay: end of journal, done." << dendl;
      break;
    }

    if (seq <= op_seq) {
      dout(3) << "journal_replay: skipping old op seq " << seq << " <= " << op_seq << dendl;
      continue;
    }
    assert(op_seq == seq-1);

    dout(3) << "journal_replay: applying op seq " << seq << dendl;
    bufferlist::iterator p = bl.begin();
    vector<ObjectStore::Transaction> tls;
    while (!p.end()) {
      tls.emplace_back(Transaction(p));
    }

    apply_manager.op_apply_start(seq);
    //int r = do_transactions(tls, seq);
    dout(10) << "Skipping do_transactions seq " << seq << dendl;
    apply_manager.op_apply_finish(seq);

    op_seq = seq;
    count++;

    dout(3) << "journal_replay: op_seq now " << op_seq << dendl;
    //dout(3) << "journal_replay: r = " << r << ", op_seq now " << op_seq << dendl;
  }

  if (count)
    dout(3) << "journal_replay: total = " << count << dendl;

  replaying = false;

  submit_manager.set_op_seq(op_seq);


  // done reading, make writeable.
  err = journal->make_writeable();
  if (err < 0)
    return err;

  return count;
}


// ------------------------------------

uint64_t BDJournalingObjectStore::ApplyManager::op_apply_start(uint64_t op)
{
  Mutex::Locker l(apply_lock);
  while (blocked) {
    dout(10) << "op_apply_start blocked, waiting" << dendl;
    blocked_cond.Wait(apply_lock);
  }
  dout(10) << "op_apply_start " << op << " open_ops " << open_ops << " -> "
	   << (open_ops+1) << dendl;
  assert(!blocked);
  assert(op > committed_seq);
  open_ops++;
  return op;
}

void BDJournalingObjectStore::ApplyManager::op_apply_finish(uint64_t op)
{
  Mutex::Locker l(apply_lock);
  dout(10) << "op_apply_finish " << op << " open_ops " << open_ops << " -> "
	   << (open_ops-1) << dendl; 

  //dout(10) << "op_apply_finish " << op << " open_ops " << open_ops << " -> "
//	   << (open_ops-1) << ", max_applied_seq " << max_applied_seq << " -> "
//	   << MAX(op, max_applied_seq) << dendl;
  --open_ops;
  assert(open_ops >= 0);

  // signal a blocked commit_start
  if (blocked) {
    blocked_cond.Signal();
  }

  // 여기에서 max_applied_seq 를 업데이트 하지 말고 
  // early_applied_seq 에 추가. 
  //
  early_applied_seq.insert(op);

  for(set<uint64_t>::iterator p = early_applied_seq.begin();
      p != early_applied_seq.end();
      p++){
    if ( (*p) == max_applied_seq_thru + 1){ 
      max_applied_seq_thru ++;
      early_applied_seq.erase(p);
    } else {
      assert(*p > max_applied_seq_thru);
      break;
    }
  }

  // there can be multiple applies in flight; track the max value we
  // note.  note that we can't _read_ this value and learn anything
  // meaningful unless/until we've quiesced all in-flight applies.
  if (op > max_applied_seq)
    max_applied_seq = op;
}

uint64_t BDJournalingObjectStore::SubmitManager::op_submit_start()
{
  lock.Lock();
  uint64_t op = ++op_seq;
  dout(10) << "op_submit_start " << op << dendl;
  return op;
}

void BDJournalingObjectStore::SubmitManager::op_submit_finish(uint64_t op)
{
  dout(10) << "op_submit_finish " << op << dendl;
  if (op != op_submitted + 1) {
    dout(0) << "op_submit_finish " << op << " expected " << (op_submitted + 1)
	    << ", OUT OF ORDER" << dendl;
    assert(0 == "out of order op_submit_finish");
  }
  op_submitted = op;
  lock.Unlock();
}


// ------------------------------------------

void BDJournalingObjectStore::ApplyManager::add_waiter(uint64_t op, Context *c)
{
  //EUNJI 
  dout(10) << __func__ << dendl;
  assert(true);
  //
  Mutex::Locker l(com_lock);
  assert(c);
  commit_waiters[op].push_back(c);
}

bool BDJournalingObjectStore::ApplyManager::commit_start()
{
  bool ret = false;

  {
    Mutex::Locker l(apply_lock);
    dout(10) << "commit_start max_applied_seq_thru " << max_applied_seq_thru
	     << ", open_ops " << open_ops << dendl;
    //dout(10) << "commit_start max_applied_seq " << max_applied_seq
    //     << ", open_ops " << open_ops << dendl;

    // 사실 이거 없어도 됨. 
    blocked = true;
    while (open_ops > 0) {
      dout(10) << "commit_start waiting for " << open_ops
	       << " open ops to drain" << dendl;
      blocked_cond.Wait(apply_lock);
    }
    assert(open_ops == 0);
    dout(10) << "commit_start blocked, all open_ops have completed" << dendl;
    {
      Mutex::Locker l(com_lock);
      if (max_applied_seq_thru == committed_seq) {
      //if (max_applied_seq == committed_seq) {
	dout(10) << "commit_start nothing to do" << dendl;
	blocked = false;
	assert(commit_waiters.empty());
	goto out;
      }

      committing_seq = max_applied_seq_thru;
      //committing_seq = max_applied_seq;

      dout(10) << "commit_start committing " << committing_seq
	       << ", still blocked" << dendl;
    }
  }
  ret = true;

  if (journal)
    journal->commit_start(committing_seq);  // tell the journal too
 out:
  return ret;
}

void BDJournalingObjectStore::ApplyManager::commit_started()
{
  Mutex::Locker l(apply_lock);
  // allow new ops. (underlying fs should now be committing all prior ops)
  dout(10) << "commit_started committing " << committing_seq << ", unblocking"
	   << dendl;
  blocked = false;
  blocked_cond.Signal();
}

void BDJournalingObjectStore::ApplyManager::commit_finish()
{
  Mutex::Locker l(com_lock);
  dout(10) << "commit_finish thru " << committing_seq << dendl;

  if (journal)
    journal->committed_thru(committing_seq);

  committed_seq = committing_seq;

  map<version_t, vector<Context*> >::iterator p = commit_waiters.begin();
  while (p != commit_waiters.end() &&
    p->first <= committing_seq) {
    dout(10) << __func__ << " finisher->queue " << committed_seq << dendl;
    //
    finisher.queue(p->second);
    commit_waiters.erase(p++);
  }
}

void BDJournalingObjectStore::_op_journal_transactions(
  bufferlist& tbl, uint32_t orig_len, uint64_t op,
  Context *onjournal, TrackedOpRef osd_op)
{
  if (osd_op.get())
    dout(10) << "op_journal_transactions " << op << " reqid_t "
             << (static_cast<OpRequest *>(osd_op.get()))->get_reqid() << dendl;
  else
    dout(10) << "op_journal_transactions " << op  << dendl;

  if (journal && journal->is_writeable()) {
    journal->submit_entry(op, tbl, orig_len, onjournal, osd_op);
  } else if (onjournal) {
    apply_manager.add_waiter(op, onjournal);
  }
}
