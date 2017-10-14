// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "PGMap.h"

#define dout_subsys ceph_subsys_mon
#include "common/debug.h"
#include "common/Formatter.h"
#include "include/ceph_features.h"
#include "include/stringify.h"

#include "osd/osd_types.h"
#include "osd/OSDMap.h"

#define dout_context g_ceph_context

// --

void PGMap::Incremental::encode(bufferlist &bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_MONENC) == 0) {
    __u8 v = 4;
    ::encode(v, bl);
    ::encode(version, bl);
    ::encode(pg_stat_updates, bl);
    ::encode(osd_stat_updates, bl);
    ::encode(osd_stat_rm, bl);
    ::encode(osdmap_epoch, bl);
    ::encode(pg_scan, bl);
    ::encode(full_ratio, bl);
    ::encode(nearfull_ratio, bl);
    ::encode(pg_remove, bl);
    return;
  }

  ENCODE_START(7, 5, bl);
  ::encode(version, bl);
  ::encode(pg_stat_updates, bl);
  ::encode(osd_stat_updates, bl);
  ::encode(osd_stat_rm, bl);
  ::encode(osdmap_epoch, bl);
  ::encode(pg_scan, bl);
  ::encode(full_ratio, bl);
  ::encode(nearfull_ratio, bl);
  ::encode(pg_remove, bl);
  ::encode(stamp, bl);
  ::encode(osd_epochs, bl);
  ENCODE_FINISH(bl);
}

void PGMap::Incremental::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(7, 5, 5, bl);
  ::decode(version, bl);
  if (struct_v < 3) {
    pg_stat_updates.clear();
    __u32 n;
    ::decode(n, bl);
    while (n--) {
      old_pg_t opgid;
      ::decode(opgid, bl);
      pg_t pgid = opgid;
      ::decode(pg_stat_updates[pgid], bl);
    }
  } else {
    ::decode(pg_stat_updates, bl);
  }
  ::decode(osd_stat_updates, bl);
  ::decode(osd_stat_rm, bl);
  ::decode(osdmap_epoch, bl);
  ::decode(pg_scan, bl);
  if (struct_v >= 2) {
    ::decode(full_ratio, bl);
    ::decode(nearfull_ratio, bl);
  }
  if (struct_v < 3) {
    pg_remove.clear();
    __u32 n;
    ::decode(n, bl);
    while (n--) {
      old_pg_t opgid;
      ::decode(opgid, bl);
      pg_remove.insert(pg_t(opgid));
    }
  } else {
    ::decode(pg_remove, bl);
  }
  if (struct_v < 4 && full_ratio == 0) {
    full_ratio = -1;
  }
  if (struct_v < 4 && nearfull_ratio == 0) {
    nearfull_ratio = -1;
  }
  if (struct_v >= 6)
    ::decode(stamp, bl);
  if (struct_v >= 7) {
    ::decode(osd_epochs, bl);
  } else {
    for (map<int32_t, osd_stat_t>::iterator i = osd_stat_updates.begin();
         i != osd_stat_updates.end();
         ++i) {
      // This isn't accurate, but will cause trimming to behave like
      // previously.
      osd_epochs.insert(make_pair(i->first, osdmap_epoch));
    }
  }
  DECODE_FINISH(bl);
}

void PGMap::Incremental::dump(Formatter *f) const
{
  f->dump_unsigned("version", version);
  f->dump_stream("stamp") << stamp;
  f->dump_unsigned("osdmap_epoch", osdmap_epoch);
  f->dump_unsigned("pg_scan_epoch", pg_scan);
  f->dump_float("full_ratio", full_ratio);
  f->dump_float("nearfull_ratio", nearfull_ratio);

  f->open_array_section("pg_stat_updates");
  for (map<pg_t,pg_stat_t>::const_iterator p = pg_stat_updates.begin(); p != pg_stat_updates.end(); ++p) {
    f->open_object_section("pg_stat");
    f->dump_stream("pgid") << p->first;
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("osd_stat_updates");
  for (map<int32_t,osd_stat_t>::const_iterator p = osd_stat_updates.begin(); p != osd_stat_updates.end(); ++p) {
    f->open_object_section("osd_stat");
    f->dump_int("osd", p->first);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("osd_stat_removals");
  for (set<int>::const_iterator p = osd_stat_rm.begin(); p != osd_stat_rm.end(); ++p)
    f->dump_int("osd", *p);
  f->close_section();

  f->open_array_section("pg_removals");
  for (set<pg_t>::const_iterator p = pg_remove.begin(); p != pg_remove.end(); ++p)
    f->dump_stream("pgid") << *p;
  f->close_section();
}

void PGMap::Incremental::generate_test_instances(list<PGMap::Incremental*>& o)
{
  o.push_back(new Incremental);
  o.push_back(new Incremental);
  o.back()->version = 1;
  o.back()->stamp = utime_t(123,345);
  o.push_back(new Incremental);
  o.back()->version = 2;
  o.back()->pg_stat_updates[pg_t(1,2,3)] = pg_stat_t();
  o.back()->osd_stat_updates[5] = osd_stat_t();
  o.back()->osd_epochs[5] = 12;
  o.push_back(new Incremental);
  o.back()->version = 3;
  o.back()->osdmap_epoch = 1;
  o.back()->pg_scan = 2;
  o.back()->full_ratio = .2;
  o.back()->nearfull_ratio = .3;
  o.back()->pg_stat_updates[pg_t(4,5,6)] = pg_stat_t();
  o.back()->osd_stat_updates[6] = osd_stat_t();
  o.back()->osd_epochs[6] = 12;
  o.back()->pg_remove.insert(pg_t(1,2,3));
  o.back()->osd_stat_rm.insert(5);
}


// --

void PGMap::apply_incremental(CephContext *cct, const Incremental& inc)
{
  assert(inc.version == version+1);
  version++;

  utime_t delta_t;
  delta_t = inc.stamp;
  delta_t -= stamp;
  stamp = inc.stamp;

  pool_stat_t pg_sum_old = pg_sum;
  ceph::unordered_map<uint64_t, pool_stat_t> pg_pool_sum_old;

  bool ratios_changed = false;
  if (inc.full_ratio != full_ratio && inc.full_ratio != -1) {
    full_ratio = inc.full_ratio;
    ratios_changed = true;
  }
  if (inc.nearfull_ratio != nearfull_ratio && inc.nearfull_ratio != -1) {
    nearfull_ratio = inc.nearfull_ratio;
    ratios_changed = true;
  }
  if (ratios_changed)
    redo_full_sets();

  for (map<pg_t,pg_stat_t>::const_iterator p = inc.pg_stat_updates.begin();
       p != inc.pg_stat_updates.end();
       ++p) {
    const pg_t &update_pg(p->first);
    const pg_stat_t &update_stat(p->second);

    if (pg_pool_sum_old.count(update_pg.pool()) == 0)
      pg_pool_sum_old[update_pg.pool()] = pg_pool_sum[update_pg.pool()];

    ceph::unordered_map<pg_t,pg_stat_t>::iterator t = pg_stat.find(update_pg);
    if (t == pg_stat.end()) {
      ceph::unordered_map<pg_t,pg_stat_t>::value_type v(update_pg, update_stat);
      pg_stat.insert(v);
    } else {
      stat_pg_sub(update_pg, t->second);
      t->second = update_stat;
    }
    stat_pg_add(update_pg, update_stat);
  }
  assert(osd_stat.size() == osd_epochs.size());
  for (map<int32_t,osd_stat_t>::const_iterator p =
         inc.get_osd_stat_updates().begin();
       p != inc.get_osd_stat_updates().end();
       ++p) {
    int osd = p->first;
    const osd_stat_t &new_stats(p->second);

    ceph::unordered_map<int32_t,osd_stat_t>::iterator t = osd_stat.find(osd);
    if (t == osd_stat.end()) {
      ceph::unordered_map<int32_t,osd_stat_t>::value_type v(osd, new_stats);
      osd_stat.insert(v);
    } else {
      stat_osd_sub(t->second);
      t->second = new_stats;
    }
    ceph::unordered_map<int32_t,epoch_t>::iterator i = osd_epochs.find(osd);
    map<int32_t,epoch_t>::const_iterator j = inc.get_osd_epochs().find(osd);
    assert(j != inc.get_osd_epochs().end());

    if (i == osd_epochs.end())
      osd_epochs.insert(*j);
    else
      i->second = j->second;

    stat_osd_add(new_stats);

    // adjust [near]full status
    register_nearfull_status(osd, new_stats);
  }
  set<int64_t> deleted_pools;
  for (set<pg_t>::const_iterator p = inc.pg_remove.begin();
       p != inc.pg_remove.end();
       ++p) {
    const pg_t &removed_pg(*p);
    ceph::unordered_map<pg_t,pg_stat_t>::iterator s = pg_stat.find(removed_pg);
    if (s != pg_stat.end()) {
      stat_pg_sub(removed_pg, s->second);
      pg_stat.erase(s);
    }
    if (removed_pg.ps() == 0)
      deleted_pools.insert(removed_pg.pool());
  }
  for (set<int64_t>::iterator p = deleted_pools.begin();
       p != deleted_pools.end();
       ++p) {
    dout(20) << " deleted pool " << *p << dendl;
    deleted_pool(*p);
  }

  for (set<int>::iterator p = inc.get_osd_stat_rm().begin();
       p != inc.get_osd_stat_rm().end();
       ++p) {
    ceph::unordered_map<int32_t,osd_stat_t>::iterator t = osd_stat.find(*p);
    if (t != osd_stat.end()) {
      stat_osd_sub(t->second);
      osd_stat.erase(t);
    }

    // remove these old osds from full/nearfull set(s), too
    nearfull_osds.erase(*p);
    full_osds.erase(*p);
  }

  // calculate a delta, and average over the last 2 deltas.
  pool_stat_t d = pg_sum;
  d.stats.sub(pg_sum_old.stats);
  pg_sum_deltas.push_back(make_pair(d, delta_t));
  stamp_delta += delta_t;

  pg_sum_delta.stats.add(d.stats);
  if (pg_sum_deltas.size() > (std::list< pair<pool_stat_t, utime_t> >::size_type)MAX(1, cct ? cct->_conf->mon_stat_smooth_intervals : 1)) {
    pg_sum_delta.stats.sub(pg_sum_deltas.front().first.stats);
    stamp_delta -= pg_sum_deltas.front().second;
    pg_sum_deltas.pop_front();
  }

  update_pool_deltas(cct, inc.stamp, pg_pool_sum_old);

  if (inc.osdmap_epoch)
    last_osdmap_epoch = inc.osdmap_epoch;
  if (inc.pg_scan)
    last_pg_scan = inc.pg_scan;

  min_last_epoch_clean = 0;  // invalidate
}

void PGMap::redo_full_sets()
{
  full_osds.clear();
  nearfull_osds.clear();
  for (ceph::unordered_map<int32_t, osd_stat_t>::iterator i = osd_stat.begin();
       i != osd_stat.end();
       ++i) {
    register_nearfull_status(i->first, i->second);
  }
}

void PGMap::register_nearfull_status(int osd, const osd_stat_t& s)
{
  float ratio = ((float)s.kb_used) / ((float)s.kb);

  if (full_ratio > 0 && ratio > full_ratio) {
    // full
    full_osds.insert(osd);
    nearfull_osds.erase(osd);
  } else if (nearfull_ratio > 0 && ratio > nearfull_ratio) {
    // nearfull
    full_osds.erase(osd);
    nearfull_osds.insert(osd);
  } else {
    // ok
    full_osds.erase(osd);
    nearfull_osds.erase(osd);
  }
}

void PGMap::calc_stats()
{
  num_pg_by_state.clear();
  num_pg = 0;
  num_osd = 0;
  pg_pool_sum.clear();
  pg_sum = pool_stat_t();
  osd_sum = osd_stat_t();
  pg_by_osd.clear();

  for (ceph::unordered_map<pg_t,pg_stat_t>::iterator p = pg_stat.begin();
       p != pg_stat.end();
       ++p) {
    stat_pg_add(p->first, p->second);
  }
  for (ceph::unordered_map<int32_t,osd_stat_t>::iterator p = osd_stat.begin();
       p != osd_stat.end();
       ++p)
    stat_osd_add(p->second);

  redo_full_sets();

  min_last_epoch_clean = calc_min_last_epoch_clean();
}

void PGMap::update_pg(pg_t pgid, bufferlist& bl)
{
  bufferlist::iterator p = bl.begin();
  ceph::unordered_map<pg_t,pg_stat_t>::iterator s = pg_stat.find(pgid);
  epoch_t old_lec = 0, lec;
  if (s != pg_stat.end()) {
    old_lec = s->second.get_effective_last_epoch_clean();
    stat_pg_update(pgid, s->second, p);
    lec = s->second.get_effective_last_epoch_clean();
  } else {
    pg_stat_t& r = pg_stat[pgid];
    ::decode(r, p);
    stat_pg_add(pgid, r);
    lec = r.get_effective_last_epoch_clean();
  }

  if (min_last_epoch_clean &&
      (lec < min_last_epoch_clean ||  // we did
       (lec > min_last_epoch_clean && // we might
        old_lec == min_last_epoch_clean)
      ))
    min_last_epoch_clean = 0;
}

void PGMap::remove_pg(pg_t pgid)
{
  ceph::unordered_map<pg_t,pg_stat_t>::iterator s = pg_stat.find(pgid);
  if (s != pg_stat.end()) {
    if (min_last_epoch_clean &&
        s->second.get_effective_last_epoch_clean() == min_last_epoch_clean)
      min_last_epoch_clean = 0;
    stat_pg_sub(pgid, s->second);
    pg_stat.erase(s);
  }
}

void PGMap::update_osd(int osd, bufferlist& bl)
{
  bufferlist::iterator p = bl.begin();
  ceph::unordered_map<int32_t,osd_stat_t>::iterator o = osd_stat.find(osd);
  epoch_t old_lec = 0;
  if (o != osd_stat.end()) {
    ceph::unordered_map<int32_t,epoch_t>::iterator i = osd_epochs.find(osd);
    if (i != osd_epochs.end())
      old_lec = i->second;
    stat_osd_sub(o->second);
  }
  osd_stat_t& r = osd_stat[osd];
  ::decode(r, p);
  stat_osd_add(r);

  // adjust [near]full status
  register_nearfull_status(osd, r);

  // epoch?
  if (!p.end()) {
    epoch_t e;
    ::decode(e, p);

    if (e < min_last_epoch_clean ||
        (e > min_last_epoch_clean &&
         old_lec == min_last_epoch_clean))
      min_last_epoch_clean = 0;
  } else {
    // WARNING: we are not refreshing min_last_epoch_clean!  must be old store
    // or old mon running.
  }
}

void PGMap::remove_osd(int osd)
{
  ceph::unordered_map<int32_t,osd_stat_t>::iterator o = osd_stat.find(osd);
  if (o != osd_stat.end()) {
    stat_osd_sub(o->second);
    osd_stat.erase(o);

    // remove these old osds from full/nearfull set(s), too
    nearfull_osds.erase(osd);
    full_osds.erase(osd);
  }
}

void PGMap::stat_pg_add(const pg_t &pgid, const pg_stat_t &s,
                        bool sameosds)
{
  pg_pool_sum[pgid.pool()].add(s);
  pg_sum.add(s);

  num_pg++;
  num_pg_by_state[s.state]++;

  if ((s.state & PG_STATE_CREATING) &&
      s.parent_split_bits == 0) {
    creating_pgs.insert(pgid);
    if (s.acting_primary >= 0) {
      creating_pgs_by_osd_epoch[s.acting_primary][s.mapping_epoch].insert(pgid);
    }
  }

  if (sameosds)
    return;

  for (vector<int>::const_iterator p = s.blocked_by.begin();
       p != s.blocked_by.end();
       ++p) {
    ++blocked_by_sum[*p];
  }

  for (vector<int>::const_iterator p = s.acting.begin(); p != s.acting.end(); ++p)
    pg_by_osd[*p].insert(pgid);
  for (vector<int>::const_iterator p = s.up.begin(); p != s.up.end(); ++p)
    pg_by_osd[*p].insert(pgid);
}

void PGMap::stat_pg_sub(const pg_t &pgid, const pg_stat_t &s,
                        bool sameosds)
{
  pool_stat_t& ps = pg_pool_sum[pgid.pool()];
  ps.sub(s);
  if (ps.is_zero())
    pg_pool_sum.erase(pgid.pool());
  pg_sum.sub(s);

  num_pg--;
  int end = --num_pg_by_state[s.state];
  assert(end >= 0);
  if (end == 0)
    num_pg_by_state.erase(s.state);

  if ((s.state & PG_STATE_CREATING) &&
      s.parent_split_bits == 0) {
    creating_pgs.erase(pgid);
    if (s.acting_primary >= 0) {
      map<epoch_t,set<pg_t> >& r = creating_pgs_by_osd_epoch[s.acting_primary];
      r[s.mapping_epoch].erase(pgid);
      if (r[s.mapping_epoch].empty())
	r.erase(s.mapping_epoch);
      if (r.empty())
	creating_pgs_by_osd_epoch.erase(s.acting_primary);
    }
  }

  if (sameosds)
    return;

  for (vector<int>::const_iterator p = s.blocked_by.begin();
       p != s.blocked_by.end();
       ++p) {
    ceph::unordered_map<int,int>::iterator q = blocked_by_sum.find(*p);
    assert(q != blocked_by_sum.end());
    --q->second;
    if (q->second == 0)
      blocked_by_sum.erase(q);
  }

  for (vector<int>::const_iterator p = s.acting.begin(); p != s.acting.end(); ++p) {
    set<pg_t>& oset = pg_by_osd[*p];
    oset.erase(pgid);
    if (oset.empty())
      pg_by_osd.erase(*p);
  }
  for (vector<int>::const_iterator p = s.up.begin(); p != s.up.end(); ++p) {
    set<pg_t>& oset = pg_by_osd[*p];
    oset.erase(pgid);
    if (oset.empty())
      pg_by_osd.erase(*p);
  }
}

void PGMap::stat_pg_update(const pg_t pgid, pg_stat_t& s,
                           bufferlist::iterator& blp)
{
  pg_stat_t n;
  ::decode(n, blp);

  bool sameosds =
    s.acting == n.acting &&
    s.up == n.up &&
    s.blocked_by == n.blocked_by;

  stat_pg_sub(pgid, s, sameosds);
  s = n;
  stat_pg_add(pgid, n, sameosds);
}

void PGMap::stat_osd_add(const osd_stat_t &s)
{
  num_osd++;
  osd_sum.add(s);
}

void PGMap::stat_osd_sub(const osd_stat_t &s)
{
  num_osd--;
  osd_sum.sub(s);
}

epoch_t PGMap::calc_min_last_epoch_clean() const
{
  if (pg_stat.empty())
    return 0;

  ceph::unordered_map<pg_t,pg_stat_t>::const_iterator p = pg_stat.begin();
  epoch_t min = p->second.get_effective_last_epoch_clean();
  for (++p; p != pg_stat.end(); ++p) {
    epoch_t lec = p->second.get_effective_last_epoch_clean();
    if (lec < min)
      min = lec;
  }
  // also scan osd epochs
  // don't trim past the oldest reported osd epoch
  for (ceph::unordered_map<int32_t, epoch_t>::const_iterator i = osd_epochs.begin();
       i != osd_epochs.end();
       ++i) {
    if (i->second < min)
      min = i->second;
  }
  return min;
}

void PGMap::encode(bufferlist &bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_MONENC) == 0) {
    __u8 v = 3;
    ::encode(v, bl);
    ::encode(version, bl);
    ::encode(pg_stat, bl);
    ::encode(osd_stat, bl);
    ::encode(last_osdmap_epoch, bl);
    ::encode(last_pg_scan, bl);
    ::encode(full_ratio, bl);
    ::encode(nearfull_ratio, bl);
    return;
  }

  ENCODE_START(6, 4, bl);
  ::encode(version, bl);
  ::encode(pg_stat, bl);
  ::encode(osd_stat, bl);
  ::encode(last_osdmap_epoch, bl);
  ::encode(last_pg_scan, bl);
  ::encode(full_ratio, bl);
  ::encode(nearfull_ratio, bl);
  ::encode(stamp, bl);
  ::encode(osd_epochs, bl);
  ENCODE_FINISH(bl);
}

void PGMap::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(6, 4, 4, bl);
  ::decode(version, bl);
  if (struct_v < 3) {
    pg_stat.clear();
    __u32 n;
    ::decode(n, bl);
    while (n--) {
      old_pg_t opgid;
      ::decode(opgid, bl);
      pg_t pgid = opgid;
      ::decode(pg_stat[pgid], bl);
    }
  } else {
    ::decode(pg_stat, bl);
  }
  ::decode(osd_stat, bl);
  ::decode(last_osdmap_epoch, bl);
  ::decode(last_pg_scan, bl);
  if (struct_v >= 2) {
    ::decode(full_ratio, bl);
    ::decode(nearfull_ratio, bl);
  }
  if (struct_v >= 5)
    ::decode(stamp, bl);
  if (struct_v >= 6) {
    ::decode(osd_epochs, bl);
  } else {
    for (ceph::unordered_map<int32_t, osd_stat_t>::iterator i = osd_stat.begin();
         i != osd_stat.end();
         ++i) {
      // This isn't accurate, but will cause trimming to behave like
      // previously.
      osd_epochs.insert(make_pair(i->first, last_osdmap_epoch));
    }
  }
  DECODE_FINISH(bl);

  calc_stats();
}

void PGMap::dirty_all(Incremental& inc)
{
  inc.osdmap_epoch = last_osdmap_epoch;
  inc.pg_scan = last_pg_scan;
  inc.full_ratio = full_ratio;
  inc.nearfull_ratio = nearfull_ratio;

  for (ceph::unordered_map<pg_t,pg_stat_t>::const_iterator p = pg_stat.begin(); p != pg_stat.end(); ++p) {
    inc.pg_stat_updates[p->first] = p->second;
  }
  for (ceph::unordered_map<int32_t, osd_stat_t>::const_iterator p = osd_stat.begin(); p != osd_stat.end(); ++p) {
    assert(osd_epochs.count(p->first));
    inc.update_stat(p->first,
                    inc.get_osd_epochs().find(p->first)->second,
                    p->second);
  }
}

void PGMap::dump(Formatter *f) const
{
  dump_basic(f);
  dump_pg_stats(f, false);
  dump_pool_stats(f);
  dump_osd_stats(f);
}

void PGMap::dump_basic(Formatter *f) const
{
  f->dump_unsigned("version", version);
  f->dump_stream("stamp") << stamp;
  f->dump_unsigned("last_osdmap_epoch", last_osdmap_epoch);
  f->dump_unsigned("last_pg_scan", last_pg_scan);
  f->dump_unsigned("min_last_epoch_clean", min_last_epoch_clean);
  f->dump_float("full_ratio", full_ratio);
  f->dump_float("near_full_ratio", nearfull_ratio);

  f->open_object_section("pg_stats_sum");
  pg_sum.dump(f);
  f->close_section();

  f->open_object_section("osd_stats_sum");
  osd_sum.dump(f);
  f->close_section();

  f->open_object_section("osd_epochs");
  for (ceph::unordered_map<int32_t,epoch_t>::const_iterator p =
         osd_epochs.begin(); p != osd_epochs.end(); ++p) {
    f->open_object_section("osd");
    f->dump_unsigned("osd", p->first);
    f->dump_unsigned("epoch", p->second);
    f->close_section();
  }
  f->close_section();

  dump_delta(f);
}

void PGMap::dump_delta(Formatter *f) const
{
  f->open_object_section("pg_stats_delta");
  pg_sum_delta.dump(f);
  f->close_section();
}

void PGMap::dump_pg_stats(Formatter *f, bool brief) const
{
  f->open_array_section("pg_stats");
  for (ceph::unordered_map<pg_t,pg_stat_t>::const_iterator i = pg_stat.begin();
       i != pg_stat.end();
       ++i) {
    f->open_object_section("pg_stat");
    f->dump_stream("pgid") << i->first;
    if (brief)
      i->second.dump_brief(f);
    else
      i->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void PGMap::dump_pool_stats(Formatter *f) const
{
  f->open_array_section("pool_stats");
  for (ceph::unordered_map<int,pool_stat_t>::const_iterator p = pg_pool_sum.begin();
       p != pg_pool_sum.end();
       ++p) {
    f->open_object_section("pool_stat");
    f->dump_int("poolid", p->first);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void PGMap::dump_osd_stats(Formatter *f) const
{
  f->open_array_section("osd_stats");
  for (ceph::unordered_map<int32_t,osd_stat_t>::const_iterator q = osd_stat.begin();
       q != osd_stat.end();
       ++q) {
    f->open_object_section("osd_stat");
    f->dump_int("osd", q->first);
    q->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void PGMap::dump_pg_stats_plain(ostream& ss,
                                const ceph::unordered_map<pg_t, pg_stat_t>& pg_stats,
                                bool brief) const
{
  TextTable tab;

  if (brief){
    tab.define_column("PG_STAT", TextTable::LEFT, TextTable::LEFT);
    tab.define_column("STATE", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("UP", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("UP_PRIMARY", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("ACTING", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("ACTING_PRIMARY", TextTable::LEFT, TextTable::RIGHT);
  }
  else {
    tab.define_column("PG_STAT", TextTable::LEFT, TextTable::LEFT);
    tab.define_column("OBJECTS", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("MISSING_ON_PRIMARY", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("DEGRADED", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("MISPLACED", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("UNFOUND", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("BYTES", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("LOG", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("DISK_LOG", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("STATE", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("STATE_STAMP", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("VERSION", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("REPORTED", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("UP", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("UP_PRIMARY", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("ACTING", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("ACTING_PRIMARY", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("LAST_SCRUB", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("SCRUB_STAMP", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("LAST_DEEP_SCRUB", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("DEEP_SCRUB_STAMP", TextTable::LEFT, TextTable::RIGHT);
  }

  for (ceph::unordered_map<pg_t, pg_stat_t>::const_iterator i = pg_stats.begin();
       i != pg_stats.end(); ++i) {
    const pg_stat_t &st(i->second);
    if (brief) {
      tab << i->first
          << pg_state_string(st.state)
          << st.up
          << st.up_primary
          << st.acting
          << st.acting_primary
          << TextTable::endrow;
    } else {
      ostringstream reported;
      reported << st.reported_epoch << ":" << st.reported_seq;

      tab << i->first
          << st.stats.sum.num_objects
          << st.stats.sum.num_objects_missing_on_primary
          << st.stats.sum.num_objects_degraded
          << st.stats.sum.num_objects_misplaced
          << st.stats.sum.num_objects_unfound
          << st.stats.sum.num_bytes
          << st.log_size
          << st.ondisk_log_size
          << pg_state_string(st.state)
          << st.last_change
          << st.version
          << reported.str()
          << pg_vector_string(st.up)
          << st.up_primary
          << pg_vector_string(st.acting)
          << st.acting_primary
          << st.last_scrub
          << st.last_scrub_stamp
          << st.last_deep_scrub
          << st.last_deep_scrub_stamp
          << TextTable::endrow;
    }
  }

  ss << tab;
}

void PGMap::dump(ostream& ss) const
{
  dump_basic(ss);
  dump_pg_stats(ss, false);
  dump_pool_stats(ss, false);
  dump_pg_sum_stats(ss, false);
  dump_osd_stats(ss);
}

void PGMap::dump_basic(ostream& ss) const
{
  ss << "version " << version << std::endl;
  ss << "stamp " << stamp << std::endl;
  ss << "last_osdmap_epoch " << last_osdmap_epoch << std::endl;
  ss << "last_pg_scan " << last_pg_scan << std::endl;
  ss << "full_ratio " << full_ratio << std::endl;
  ss << "nearfull_ratio " << nearfull_ratio << std::endl;
}

void PGMap::dump_pg_stats(ostream& ss, bool brief) const
{
  dump_pg_stats_plain(ss, pg_stat, brief);
}

void PGMap::dump_pool_stats(ostream& ss, bool header) const
{
  TextTable tab;

  if (header) {
    tab.define_column("POOLID", TextTable::LEFT, TextTable::LEFT);
    tab.define_column("OBJECTS", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("MISSING_ON_PRIMARY", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("DEGRADED", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("MISPLACED", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("UNFOUND", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("BYTES", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("LOG", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("DISK_LOG", TextTable::LEFT, TextTable::RIGHT);
  } else {
    tab.define_column("", TextTable::LEFT, TextTable::LEFT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
  }

  for (ceph::unordered_map<int,pool_stat_t>::const_iterator p = pg_pool_sum.begin();
       p != pg_pool_sum.end();
       ++p) {
    tab << p->first
        << p->second.stats.sum.num_objects
        << p->second.stats.sum.num_objects_missing_on_primary
        << p->second.stats.sum.num_objects_degraded
        << p->second.stats.sum.num_objects_misplaced
        << p->second.stats.sum.num_objects_unfound
        << p->second.stats.sum.num_bytes
        << p->second.log_size
        << p->second.ondisk_log_size
        << TextTable::endrow;
  }

  ss << tab;
}

void PGMap::dump_pg_sum_stats(ostream& ss, bool header) const
{
  TextTable tab;

  if (header) {
    tab.define_column("PG_STAT", TextTable::LEFT, TextTable::LEFT);
    tab.define_column("OBJECTS", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("MISSING_ON_PRIMARY", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("DEGRADED", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("MISPLACED", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("UNFOUND", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("BYTES", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("LOG", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("DISK_LOG", TextTable::LEFT, TextTable::RIGHT);
  } else {
    tab.define_column("", TextTable::LEFT, TextTable::LEFT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
  };

  tab << "sum"
      << pg_sum.stats.sum.num_objects
      << pg_sum.stats.sum.num_objects_missing_on_primary
      << pg_sum.stats.sum.num_objects_degraded
      << pg_sum.stats.sum.num_objects_misplaced
      << pg_sum.stats.sum.num_objects_unfound
      << pg_sum.stats.sum.num_bytes
      << pg_sum.log_size
      << pg_sum.ondisk_log_size
      << TextTable::endrow;

  ss << tab;
}

void PGMap::dump_osd_stats(ostream& ss) const
{
  TextTable tab;

  tab.define_column("OSD_STAT", TextTable::LEFT, TextTable::LEFT);
  tab.define_column("USED", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("AVAIL", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("TOTAL", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("HB_PEERS", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("PG_SUM", TextTable::LEFT, TextTable::RIGHT);

  for (ceph::unordered_map<int32_t,osd_stat_t>::const_iterator p = osd_stat.begin();
       p != osd_stat.end();
       ++p) {
    tab << p->first
        << si_t(p->second.kb_used << 10)
        << si_t(p->second.kb_avail << 10)
        << si_t(p->second.kb << 10)
        << p->second.hb_peers
        << get_num_pg_by_osd(p->first)
        << TextTable::endrow;
  }

  tab << "sum"
      << si_t(osd_sum.kb_used << 10)
      << si_t(osd_sum.kb_avail << 10)
      << si_t(osd_sum.kb << 10)
      << TextTable::endrow;

  ss << tab;
}

void PGMap::dump_osd_sum_stats(ostream& ss) const
{
  TextTable tab;

  tab.define_column("OSD_STAT", TextTable::LEFT, TextTable::LEFT);
  tab.define_column("USED", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("AVAIL", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("TOTAL", TextTable::LEFT, TextTable::RIGHT);

  tab << "sum"
      << si_t(osd_sum.kb_used << 10)
      << si_t(osd_sum.kb_avail << 10)
      << si_t(osd_sum.kb << 10)
      << TextTable::endrow;

  ss << tab;
}

void PGMap::get_stuck_stats(int types, const utime_t cutoff,
                            ceph::unordered_map<pg_t, pg_stat_t>& stuck_pgs) const
{
  assert(types != 0);
  for (ceph::unordered_map<pg_t, pg_stat_t>::const_iterator i = pg_stat.begin();
       i != pg_stat.end();
       ++i) {
    utime_t val = cutoff; // don't care about >= cutoff so that is infinity

    if ((types & STUCK_INACTIVE) && !(i->second.state & PG_STATE_ACTIVE)) {
      if (i->second.last_active < val)
	val = i->second.last_active;
    }

    if ((types & STUCK_UNCLEAN) && !(i->second.state & PG_STATE_CLEAN)) {
      if (i->second.last_clean < val)
	val = i->second.last_clean;
    }

    if ((types & STUCK_DEGRADED) && (i->second.state & PG_STATE_DEGRADED)) {
      if (i->second.last_undegraded < val)
	val = i->second.last_undegraded;
    }

    if ((types & STUCK_UNDERSIZED) && (i->second.state & PG_STATE_UNDERSIZED)) {
      if (i->second.last_fullsized < val)
	val = i->second.last_fullsized;
    }

    if ((types & STUCK_STALE) && (i->second.state & PG_STATE_STALE)) {
      if (i->second.last_unstale < val)
	val = i->second.last_unstale;
    }

    // val is now the earliest any of the requested stuck states began
    if (val < cutoff) {
      stuck_pgs[i->first] = i->second;
    }
  }
}

bool PGMap::get_stuck_counts(const utime_t cutoff, map<string, int>& note) const
{
  int inactive = 0;
  int unclean = 0;
  int degraded = 0;
  int undersized = 0;
  int stale = 0;

  for (ceph::unordered_map<pg_t, pg_stat_t>::const_iterator i = pg_stat.begin();
       i != pg_stat.end();
       ++i) {
    if (! (i->second.state & PG_STATE_ACTIVE)) {
      if (i->second.last_active < cutoff)
        ++inactive;
    }
    if (! (i->second.state & PG_STATE_CLEAN)) {
      if (i->second.last_clean < cutoff)
        ++unclean;
    }
    if (i->second.state & PG_STATE_DEGRADED) {
      if (i->second.last_undegraded < cutoff)
        ++degraded;
    }
    if (i->second.state & PG_STATE_UNDERSIZED) {
      if (i->second.last_fullsized < cutoff)
        ++undersized;
    }
    if (i->second.state & PG_STATE_STALE) {
      if (i->second.last_unstale < cutoff)
        ++stale;
    }
  }
  
  if (inactive)
    note["stuck inactive"] = inactive;
  
  if (unclean)
    note["stuck unclean"] = unclean;
  
  if (undersized)
    note["stuck undersized"] = undersized;
  
  if (degraded)
    note["stuck degraded"] = degraded;
  
  if (stale)
    note["stuck stale"] = stale; 
  
  return inactive || unclean || undersized || degraded || stale;
}

void PGMap::dump_stuck(Formatter *f, int types, utime_t cutoff) const
{
  ceph::unordered_map<pg_t, pg_stat_t> stuck_pg_stats;
  get_stuck_stats(types, cutoff, stuck_pg_stats);
  f->open_array_section("stuck_pg_stats");
  for (ceph::unordered_map<pg_t,pg_stat_t>::const_iterator i = stuck_pg_stats.begin();
       i != stuck_pg_stats.end();
       ++i) {
    f->open_object_section("pg_stat");
    f->dump_stream("pgid") << i->first;
    i->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void PGMap::dump_stuck_plain(ostream& ss, int types, utime_t cutoff) const
{
  ceph::unordered_map<pg_t, pg_stat_t> stuck_pg_stats;
  get_stuck_stats(types, cutoff, stuck_pg_stats);
  if (!stuck_pg_stats.empty())
    dump_pg_stats_plain(ss, stuck_pg_stats, true);
}

int PGMap::dump_stuck_pg_stats(
  stringstream &ds,
  Formatter *f,
  int threshold,
  vector<string>& args) const
{
  int stuck_types = 0;

  for (vector<string>::iterator i = args.begin(); i != args.end(); ++i) {
    if (*i == "inactive")
      stuck_types |= PGMap::STUCK_INACTIVE;
    else if (*i == "unclean")
      stuck_types |= PGMap::STUCK_UNCLEAN;
    else if (*i == "undersized")
      stuck_types |= PGMap::STUCK_UNDERSIZED;
    else if (*i == "degraded")
      stuck_types |= PGMap::STUCK_DEGRADED;
    else if (*i == "stale")
      stuck_types |= PGMap::STUCK_STALE;
    else {
      ds << "Unknown type: " << *i << std::endl;
      return -EINVAL;
    }
  }

  utime_t now(ceph_clock_now());
  utime_t cutoff = now - utime_t(threshold, 0);

  if (!f) {
    dump_stuck_plain(ds, stuck_types, cutoff);
  } else {
    dump_stuck(f, stuck_types, cutoff);
    f->flush(ds);
  }

  return 0;
}

void PGMap::dump_osd_perf_stats(Formatter *f) const
{
  f->open_array_section("osd_perf_infos");
  for (ceph::unordered_map<int32_t, osd_stat_t>::const_iterator i = osd_stat.begin();
       i != osd_stat.end();
       ++i) {
    f->open_object_section("osd");
    f->dump_int("id", i->first);
    {
      f->open_object_section("perf_stats");
      i->second.os_perf_stat.dump(f);
      f->close_section();
    }
    f->close_section();
  }
  f->close_section();
}
void PGMap::print_osd_perf_stats(std::ostream *ss) const
{
  TextTable tab;
  tab.define_column("osd", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("commit_latency(ms)", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("apply_latency(ms)", TextTable::LEFT, TextTable::RIGHT);
  for (ceph::unordered_map<int32_t, osd_stat_t>::const_iterator i = osd_stat.begin();
       i != osd_stat.end();
       ++i) {
    tab << i->first;
    tab << i->second.os_perf_stat.os_commit_latency;
    tab << i->second.os_perf_stat.os_apply_latency;
    tab << TextTable::endrow;
  }
  (*ss) << tab;
}

void PGMap::dump_osd_blocked_by_stats(Formatter *f) const
{
  f->open_array_section("osd_blocked_by_infos");
  for (ceph::unordered_map<int,int>::const_iterator i = blocked_by_sum.begin();
       i != blocked_by_sum.end();
       ++i) {
    f->open_object_section("osd");
    f->dump_int("id", i->first);
    f->dump_int("num_blocked", i->second);
    f->close_section();
  }
  f->close_section();
}
void PGMap::print_osd_blocked_by_stats(std::ostream *ss) const
{
  TextTable tab;
  tab.define_column("osd", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("num_blocked", TextTable::LEFT, TextTable::RIGHT);
  for (ceph::unordered_map<int,int>::const_iterator i = blocked_by_sum.begin();
       i != blocked_by_sum.end();
       ++i) {
    tab << i->first;
    tab << i->second;
    tab << TextTable::endrow;
  }
  (*ss) << tab;
}

void PGMap::recovery_summary(Formatter *f, list<string> *psl,
                             const pool_stat_t& delta_sum) const
{
  if (delta_sum.stats.sum.num_objects_degraded && delta_sum.stats.sum.num_object_copies > 0) {
    double pc = (double)delta_sum.stats.sum.num_objects_degraded /
                (double)delta_sum.stats.sum.num_object_copies * (double)100.0;
    char b[20];
    snprintf(b, sizeof(b), "%.3lf", pc);
    if (f) {
      f->dump_unsigned("degraded_objects", delta_sum.stats.sum.num_objects_degraded);
      f->dump_unsigned("degraded_total", delta_sum.stats.sum.num_object_copies);
      f->dump_float("degraded_ratio", pc / 100.0);
    } else {
      ostringstream ss;
      ss << delta_sum.stats.sum.num_objects_degraded
         << "/" << delta_sum.stats.sum.num_object_copies << " objects degraded (" << b << "%)";
      psl->push_back(ss.str());
    }
  }
  if (delta_sum.stats.sum.num_objects_misplaced && delta_sum.stats.sum.num_object_copies > 0) {
    double pc = (double)delta_sum.stats.sum.num_objects_misplaced /
                (double)delta_sum.stats.sum.num_object_copies * (double)100.0;
    char b[20];
    snprintf(b, sizeof(b), "%.3lf", pc);
    if (f) {
      f->dump_unsigned("misplaced_objects", delta_sum.stats.sum.num_objects_misplaced);
      f->dump_unsigned("misplaced_total", delta_sum.stats.sum.num_object_copies);
      f->dump_float("misplaced_ratio", pc / 100.0);
    } else {
      ostringstream ss;
      ss << delta_sum.stats.sum.num_objects_misplaced
         << "/" << delta_sum.stats.sum.num_object_copies << " objects misplaced (" << b << "%)";
      psl->push_back(ss.str());
    }
  }
  if (delta_sum.stats.sum.num_objects_unfound && delta_sum.stats.sum.num_objects) {
    double pc = (double)delta_sum.stats.sum.num_objects_unfound /
                (double)delta_sum.stats.sum.num_objects * (double)100.0;
    char b[20];
    snprintf(b, sizeof(b), "%.3lf", pc);
    if (f) {
      f->dump_unsigned("unfound_objects", delta_sum.stats.sum.num_objects_unfound);
      f->dump_unsigned("unfound_total", delta_sum.stats.sum.num_objects);
      f->dump_float("unfound_ratio", pc / 100.0);
    } else {
      ostringstream ss;
      ss << delta_sum.stats.sum.num_objects_unfound
         << "/" << delta_sum.stats.sum.num_objects << " unfound (" << b << "%)";
      psl->push_back(ss.str());
    }
  }
}

void PGMap::recovery_rate_summary(Formatter *f, ostream *out,
                                  const pool_stat_t& delta_sum,
                                  utime_t delta_stamp) const
{
  // make non-negative; we can get negative values if osds send
  // uncommitted stats and then "go backward" or if they are just
  // buggy/wrong.
  pool_stat_t pos_delta = delta_sum;
  pos_delta.floor(0);
  if (pos_delta.stats.sum.num_objects_recovered ||
      pos_delta.stats.sum.num_bytes_recovered ||
      pos_delta.stats.sum.num_keys_recovered) {
    int64_t objps = pos_delta.stats.sum.num_objects_recovered / (double)delta_stamp;
    int64_t bps = pos_delta.stats.sum.num_bytes_recovered / (double)delta_stamp;
    int64_t kps = pos_delta.stats.sum.num_keys_recovered / (double)delta_stamp;
    if (f) {
      f->dump_int("recovering_objects_per_sec", objps);
      f->dump_int("recovering_bytes_per_sec", bps);
      f->dump_int("recovering_keys_per_sec", kps);
      f->dump_int("num_objects_recovered", pos_delta.stats.sum.num_objects_recovered);
      f->dump_int("num_bytes_recovered", pos_delta.stats.sum.num_bytes_recovered);
      f->dump_int("num_keys_recovered", pos_delta.stats.sum.num_keys_recovered);
    } else {
      *out << pretty_si_t(bps) << "B/s";
      if (pos_delta.stats.sum.num_keys_recovered)
	*out << ", " << pretty_si_t(kps) << "keys/s";
      *out << ", " << pretty_si_t(objps) << "objects/s";
    }
  }
}

void PGMap::overall_recovery_rate_summary(Formatter *f, ostream *out) const
{
  recovery_rate_summary(f, out, pg_sum_delta, stamp_delta);
}

void PGMap::overall_recovery_summary(Formatter *f, list<string> *psl) const
{
  recovery_summary(f, psl, pg_sum);
}

void PGMap::pool_recovery_rate_summary(Formatter *f, ostream *out,
                                       uint64_t poolid) const
{
  ceph::unordered_map<uint64_t,pair<pool_stat_t,utime_t> >::const_iterator p =
    per_pool_sum_delta.find(poolid);
  if (p == per_pool_sum_delta.end())
    return;

  ceph::unordered_map<uint64_t,utime_t>::const_iterator ts =
    per_pool_sum_deltas_stamps.find(p->first);
  assert(ts != per_pool_sum_deltas_stamps.end());
  recovery_rate_summary(f, out, p->second.first, ts->second);
}

void PGMap::pool_recovery_summary(Formatter *f, list<string> *psl,
                                  uint64_t poolid) const
{
  ceph::unordered_map<uint64_t,pair<pool_stat_t,utime_t> >::const_iterator p =
    per_pool_sum_delta.find(poolid);
  if (p == per_pool_sum_delta.end())
    return;

  recovery_summary(f, psl, p->second.first);
}

void PGMap::client_io_rate_summary(Formatter *f, ostream *out,
                                   const pool_stat_t& delta_sum,
                                   utime_t delta_stamp) const
{
  pool_stat_t pos_delta = delta_sum;
  pos_delta.floor(0);
  if (pos_delta.stats.sum.num_rd ||
      pos_delta.stats.sum.num_wr) {
    if (pos_delta.stats.sum.num_rd) {
      int64_t rd = (pos_delta.stats.sum.num_rd_kb << 10) / (double)delta_stamp;
      if (f) {
	f->dump_int("read_bytes_sec", rd);
      } else {
	*out << pretty_si_t(rd) << "B/s rd, ";
      }
    }
    if (pos_delta.stats.sum.num_wr) {
      int64_t wr = (pos_delta.stats.sum.num_wr_kb << 10) / (double)delta_stamp;
      if (f) {
	f->dump_int("write_bytes_sec", wr);
      } else {
	*out << pretty_si_t(wr) << "B/s wr, ";
      }
    }
    int64_t iops_rd = pos_delta.stats.sum.num_rd / (double)delta_stamp;
    int64_t iops_wr = pos_delta.stats.sum.num_wr / (double)delta_stamp;
    if (f) {
      f->dump_int("read_op_per_sec", iops_rd);
      f->dump_int("write_op_per_sec", iops_wr);
    } else {
      *out << pretty_si_t(iops_rd) << "op/s rd, " << pretty_si_t(iops_wr) << "op/s wr";
    }
  }
}

void PGMap::overall_client_io_rate_summary(Formatter *f, ostream *out) const
{
  client_io_rate_summary(f, out, pg_sum_delta, stamp_delta);
}

void PGMap::pool_client_io_rate_summary(Formatter *f, ostream *out,
                                        uint64_t poolid) const
{
  ceph::unordered_map<uint64_t,pair<pool_stat_t,utime_t> >::const_iterator p =
    per_pool_sum_delta.find(poolid);
  if (p == per_pool_sum_delta.end())
    return;

  ceph::unordered_map<uint64_t,utime_t>::const_iterator ts =
    per_pool_sum_deltas_stamps.find(p->first);
  assert(ts != per_pool_sum_deltas_stamps.end());
  client_io_rate_summary(f, out, p->second.first, ts->second);
}

void PGMap::cache_io_rate_summary(Formatter *f, ostream *out,
                                  const pool_stat_t& delta_sum,
                                  utime_t delta_stamp) const
{
  pool_stat_t pos_delta = delta_sum;
  pos_delta.floor(0);
  bool have_output = false;

  if (pos_delta.stats.sum.num_flush) {
    int64_t flush = (pos_delta.stats.sum.num_flush_kb << 10) / (double)delta_stamp;
    if (f) {
      f->dump_int("flush_bytes_sec", flush);
    } else {
      *out << pretty_si_t(flush) << "B/s flush";
      have_output = true;
    }
  }
  if (pos_delta.stats.sum.num_evict) {
    int64_t evict = (pos_delta.stats.sum.num_evict_kb << 10) / (double)delta_stamp;
    if (f) {
      f->dump_int("evict_bytes_sec", evict);
    } else {
      if (have_output)
	*out << ", ";
      *out << pretty_si_t(evict) << "B/s evict";
      have_output = true;
    }
  }
  if (pos_delta.stats.sum.num_promote) {
    int64_t promote = pos_delta.stats.sum.num_promote / (double)delta_stamp;
    if (f) {
      f->dump_int("promote_op_per_sec", promote);
    } else {
      if (have_output)
	*out << ", ";
      *out << pretty_si_t(promote) << "op/s promote";
      have_output = true;
    }
  }
  if (pos_delta.stats.sum.num_flush_mode_low) {
    if (f) {
      f->dump_int("num_flush_mode_low", pos_delta.stats.sum.num_flush_mode_low);
    } else {
      if (have_output)
	*out << ", ";
      *out << pretty_si_t(pos_delta.stats.sum.num_flush_mode_low) << "PG(s) flushing";
      have_output = true;
    }
  }
  if (pos_delta.stats.sum.num_flush_mode_high) {
    if (f) {
      f->dump_int("num_flush_mode_high", pos_delta.stats.sum.num_flush_mode_high);
    } else {
      if (have_output)
	*out << ", ";
      *out << pretty_si_t(pos_delta.stats.sum.num_flush_mode_high) << "PG(s) flushing (high)";
      have_output = true;
    }
  }
  if (pos_delta.stats.sum.num_evict_mode_some) {
    if (f) {
      f->dump_int("num_evict_mode_some", pos_delta.stats.sum.num_evict_mode_some);
    } else {
      if (have_output)
	*out << ", ";
      *out << pretty_si_t(pos_delta.stats.sum.num_evict_mode_some) << "PG(s) evicting";
      have_output = true;
    }
  }
  if (pos_delta.stats.sum.num_evict_mode_full) {
    if (f) {
      f->dump_int("num_evict_mode_full", pos_delta.stats.sum.num_evict_mode_full);
    } else {
      if (have_output)
	*out << ", ";
      *out << pretty_si_t(pos_delta.stats.sum.num_evict_mode_full) << "PG(s) evicting (full)";
    }
  }
}

void PGMap::overall_cache_io_rate_summary(Formatter *f, ostream *out) const
{
  cache_io_rate_summary(f, out, pg_sum_delta, stamp_delta);
}

void PGMap::pool_cache_io_rate_summary(Formatter *f, ostream *out,
                                       uint64_t poolid) const
{
  ceph::unordered_map<uint64_t,pair<pool_stat_t,utime_t> >::const_iterator p =
    per_pool_sum_delta.find(poolid);
  if (p == per_pool_sum_delta.end())
    return;

  ceph::unordered_map<uint64_t,utime_t>::const_iterator ts =
    per_pool_sum_deltas_stamps.find(p->first);
  assert(ts != per_pool_sum_deltas_stamps.end());
  cache_io_rate_summary(f, out, p->second.first, ts->second);
}

/**
 * update aggregated delta
 *
 * @param cct               ceph context
 * @param ts                Timestamp for the stats being delta'ed
 * @param old_pool_sum      Previous stats sum
 * @param last_ts           Last timestamp for pool
 * @param result_pool_sum   Resulting stats
 * @param result_pool_delta Resulting pool delta
 * @param result_ts_delta   Resulting timestamp delta
 * @param delta_avg_list    List of last N computed deltas, used to average
 */
void PGMap::update_delta(CephContext *cct,
                         const utime_t ts,
                         const pool_stat_t& old_pool_sum,
                         utime_t *last_ts,
                         const pool_stat_t& current_pool_sum,
                         pool_stat_t *result_pool_delta,
                         utime_t *result_ts_delta,
                         list<pair<pool_stat_t,utime_t> > *delta_avg_list)
{
  /* @p ts is the timestamp we want to associate with the data
   * in @p old_pool_sum, and on which we will base ourselves to
   * calculate the delta, stored in 'delta_t'.
   */
  utime_t delta_t;
  delta_t = ts;         // start with the provided timestamp
  delta_t -= *last_ts;  // take the last timestamp we saw
  *last_ts = ts;        // @p ts becomes the last timestamp we saw

  // calculate a delta, and average over the last 2 deltas.
  /* start by taking a copy of our current @p result_pool_sum, and by
   * taking out the stats from @p old_pool_sum.  This generates a stats
   * delta.  Stash this stats delta in @p delta_avg_list, along with the
   * timestamp delta for these results.
   */
  pool_stat_t d = current_pool_sum;
  d.stats.sub(old_pool_sum.stats);
  delta_avg_list->push_back(make_pair(d,delta_t));
  *result_ts_delta += delta_t;

  /* Aggregate current delta, and take out the last seen delta (if any) to
   * average it out.
   */
  result_pool_delta->stats.add(d.stats);
  size_t s = MAX(1, cct ? cct->_conf->mon_stat_smooth_intervals : 1);
  if (delta_avg_list->size() > s) {
    result_pool_delta->stats.sub(delta_avg_list->front().first.stats);
    *result_ts_delta -= delta_avg_list->front().second;
    delta_avg_list->pop_front();
  }
}

/**
 * update aggregated delta
 *
 * @param cct            ceph context
 * @param ts             Timestamp
 * @param pg_sum_old     Old pg_sum
 */
void PGMap::update_global_delta(CephContext *cct,
                                const utime_t ts, const pool_stat_t& pg_sum_old)
{
  update_delta(cct, ts, pg_sum_old, &stamp, pg_sum, &pg_sum_delta,
               &stamp_delta, &pg_sum_deltas);
}

/**
 * Update a given pool's deltas
 *
 * @param cct           Ceph Context
 * @param ts            Timestamp for the stats being delta'ed
 * @param pool          Pool's id
 * @param old_pool_sum  Previous stats sum
 */
void PGMap::update_one_pool_delta(CephContext *cct,
                                  const utime_t ts,
                                  const uint64_t pool,
                                  const pool_stat_t& old_pool_sum)
{
  if (per_pool_sum_deltas.count(pool) == 0) {
    assert(per_pool_sum_deltas_stamps.count(pool) == 0);
    assert(per_pool_sum_delta.count(pool) == 0);
  }

  pair<pool_stat_t,utime_t>& sum_delta = per_pool_sum_delta[pool];

  update_delta(cct, ts, old_pool_sum, &sum_delta.second, pg_pool_sum[pool],
               &sum_delta.first, &per_pool_sum_deltas_stamps[pool],
               &per_pool_sum_deltas[pool]);
}

/**
 * Update pools' deltas
 *
 * @param cct               CephContext
 * @param ts                Timestamp for the stats being delta'ed
 * @param pg_pool_sum_old   Map of pool stats for delta calcs.
 */
void PGMap::update_pool_deltas(CephContext *cct, const utime_t ts,
                               const ceph::unordered_map<uint64_t,pool_stat_t>& pg_pool_sum_old)
{
  for (ceph::unordered_map<uint64_t,pool_stat_t>::const_iterator it = pg_pool_sum_old.begin();
       it != pg_pool_sum_old.end(); ++it) {
    update_one_pool_delta(cct, ts, it->first, it->second);
  }
}

void PGMap::clear_delta()
{
  pg_sum_delta = pool_stat_t();
  pg_sum_deltas.clear();
  stamp_delta = utime_t();
}

void PGMap::print_summary(Formatter *f, ostream *out) const
{
  std::stringstream ss;
  if (f)
    f->open_array_section("pgs_by_state");

  // list is descending numeric order (by count)
  multimap<int,int> state_by_count;  // count -> state
  for (ceph::unordered_map<int,int>::const_iterator p = num_pg_by_state.begin();
       p != num_pg_by_state.end();
       ++p) {
    state_by_count.insert(make_pair(p->second, p->first));
  }
  for (multimap<int,int>::reverse_iterator p = state_by_count.rbegin();
       p != state_by_count.rend();
       ++p) {
    if (f) {
      f->open_object_section("pgs_by_state_element");
      f->dump_string("state_name", pg_state_string(p->second));
      f->dump_unsigned("count", p->first);
      f->close_section();
    } else {
      ss.setf(std::ios::right);
      ss << "             " << std::setw(7) << p->first
         << " " << pg_state_string(p->second) << "\n";
      ss.unsetf(std::ios::right);
    }
  }
  if (f)
    f->close_section();

  if (f) {
    f->dump_unsigned("version", version);
    f->dump_unsigned("num_pgs", pg_stat.size());
    f->dump_unsigned("num_pools", pg_pool_sum.size());
    f->dump_unsigned("num_objects", pg_sum.stats.sum.num_objects);
    f->dump_unsigned("data_bytes", pg_sum.stats.sum.num_bytes);
    f->dump_unsigned("bytes_used", osd_sum.kb_used * 1024ull);
    f->dump_unsigned("bytes_avail", osd_sum.kb_avail * 1024ull);
    f->dump_unsigned("bytes_total", osd_sum.kb * 1024ull);
  } else {
    *out << "      pgmap v" << version << ": "
         << pg_stat.size() << " pgs, " << pg_pool_sum.size() << " pools, "
         << prettybyte_t(pg_sum.stats.sum.num_bytes) << " data, "
         << si_t(pg_sum.stats.sum.num_objects) << " objects\n";
    *out << "            "
         << kb_t(osd_sum.kb_used) << " used, "
         << kb_t(osd_sum.kb_avail) << " / "
         << kb_t(osd_sum.kb) << " avail\n";
  }

  list<string> sl;
  overall_recovery_summary(f, &sl);
  if (!f && !sl.empty()) {
    for (list<string>::iterator p = sl.begin(); p != sl.end(); ++p)
      *out << "            " << *p << "\n";
  }
  sl.clear();

  if (!f)
    *out << ss.str();   // pgs by state

  ostringstream ssr;
  overall_recovery_rate_summary(f, &ssr);
  if (!f && ssr.str().length())
    *out << "recovery io " << ssr.str() << "\n";

  ssr.clear();
  ssr.str("");

  overall_client_io_rate_summary(f, &ssr);
  if (!f && ssr.str().length())
    *out << "  client io " << ssr.str() << "\n";

  ssr.clear();
  ssr.str("");

  overall_cache_io_rate_summary(f, &ssr);
  if (!f && ssr.str().length())
    *out << "   cache io " << ssr.str() << "\n";
}

void PGMap::print_oneline_summary(Formatter *f, ostream *out) const
{
  std::stringstream ss;

  if (f)
    f->open_array_section("num_pg_by_state");
  for (ceph::unordered_map<int,int>::const_iterator p = num_pg_by_state.begin();
       p != num_pg_by_state.end();
       ++p) {
    if (f) {
      f->open_object_section("state");
      f->dump_string("name", pg_state_string(p->first));
      f->dump_unsigned("num", p->second);
      f->close_section();
    }
    if (p != num_pg_by_state.begin())
      ss << ", ";
    ss << p->second << " " << pg_state_string(p->first);
  }
  if (f)
    f->close_section();

  string states = ss.str();
  if (out)
    *out << "v" << version << ": "
         << pg_stat.size() << " pgs: "
         << states << "; "
         << prettybyte_t(pg_sum.stats.sum.num_bytes) << " data, "
         << kb_t(osd_sum.kb_used) << " used, "
         << kb_t(osd_sum.kb_avail) << " / "
         << kb_t(osd_sum.kb) << " avail";
  if (f) {
    f->dump_unsigned("version", version);
    f->dump_unsigned("num_pgs", pg_stat.size());
    f->dump_unsigned("num_bytes", pg_sum.stats.sum.num_bytes);
    f->dump_unsigned("raw_bytes_used", osd_sum.kb_used << 10);
    f->dump_unsigned("raw_bytes_avail", osd_sum.kb_avail << 10);
    f->dump_unsigned("raw_bytes", osd_sum.kb << 10);
  }

  // make non-negative; we can get negative values if osds send
  // uncommitted stats and then "go backward" or if they are just
  // buggy/wrong.
  pool_stat_t pos_delta = pg_sum_delta;
  pos_delta.floor(0);
  if (pos_delta.stats.sum.num_rd ||
      pos_delta.stats.sum.num_wr) {
    if (out)
      *out << "; ";
    if (pos_delta.stats.sum.num_rd) {
      int64_t rd = (pos_delta.stats.sum.num_rd_kb << 10) / (double)stamp_delta;
      if (out)
	*out << pretty_si_t(rd) << "B/s rd, ";
      if (f)
	f->dump_unsigned("read_bytes_sec", rd);
    }
    if (pos_delta.stats.sum.num_wr) {
      int64_t wr = (pos_delta.stats.sum.num_wr_kb << 10) / (double)stamp_delta;
      if (out)
	*out << pretty_si_t(wr) << "B/s wr, ";
      if (f)
	f->dump_unsigned("write_bytes_sec", wr);
    }
    int64_t iops = (pos_delta.stats.sum.num_rd + pos_delta.stats.sum.num_wr) / (double)stamp_delta;
    if (out)
      *out << pretty_si_t(iops) << "op/s";
    if (f)
      f->dump_unsigned("io_sec", iops);
  }

  list<string> sl;
  overall_recovery_summary(f, &sl);
  if (out)
    for (list<string>::iterator p = sl.begin(); p != sl.end(); ++p)
      *out << "; " << *p;
  std::stringstream ssr;
  overall_recovery_rate_summary(f, &ssr);
  if (out && ssr.str().length())
    *out << "; " << ssr.str() << " recovering";
}

void PGMap::generate_test_instances(list<PGMap*>& o)
{
  o.push_back(new PGMap);
  list<Incremental*> inc;
  Incremental::generate_test_instances(inc);
  delete inc.front();
  inc.pop_front();
  while (!inc.empty()) {
    PGMap *pmp = new PGMap();
    *pmp = *o.back();
    o.push_back(pmp);
    o.back()->apply_incremental(NULL, *inc.front());
    delete inc.front();
    inc.pop_front();
  }
}

void PGMap::get_filtered_pg_stats(uint32_t state, int64_t poolid, int64_t osdid,
                                  bool primary, set<pg_t>& pgs) const
{
  for (ceph::unordered_map<pg_t, pg_stat_t>::const_iterator i = pg_stat.begin();
       i != pg_stat.end();
       ++i) {
    if ((poolid >= 0) && (uint64_t(poolid) != i->first.pool()))
      continue;
    if ((osdid >= 0) && !(i->second.is_acting_osd(osdid,primary)))
      continue;
    if (!(i->second.state & state))
      continue;
    pgs.insert(i->first);
  }
}

void PGMap::dump_filtered_pg_stats(Formatter *f, set<pg_t>& pgs) const
{
  f->open_array_section("pg_stats");
  for (set<pg_t>::iterator i = pgs.begin(); i != pgs.end(); ++i) {
    const pg_stat_t& st = pg_stat.at(*i);
    f->open_object_section("pg_stat");
    f->dump_stream("pgid") << *i;
    st.dump(f);
    f->close_section();
  }
  f->close_section();
}

void PGMap::dump_filtered_pg_stats(ostream& ss, set<pg_t>& pgs) const
{
  TextTable tab;

  tab.define_column("PG_STAT", TextTable::LEFT, TextTable::LEFT);
  tab.define_column("OBJECTS", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("MISSING_ON_PRIMARY", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("DEGRADED", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("MISPLACED", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("UNFOUND", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("BYTES", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("LOG", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("DISK_LOG", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("STATE", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("STATE_STAMP", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("VERSION", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("REPORTED", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("UP", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("UP_PRIMARY", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("ACTING", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("ACTING_PRIMARY", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("LAST_SCRUB", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("SCRUB_STAMP", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("LAST_DEEP_SCRUB", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("DEEP_SCRUB_STAMP", TextTable::LEFT, TextTable::RIGHT);

  for (set<pg_t>::iterator i = pgs.begin(); i != pgs.end(); ++i) {
    const pg_stat_t& st = pg_stat.at(*i);

    ostringstream reported;
    reported << st.reported_epoch << ":" << st.reported_seq;

    tab << *i
        << st.stats.sum.num_objects
        << st.stats.sum.num_objects_missing_on_primary
        << st.stats.sum.num_objects_degraded
        << st.stats.sum.num_objects_misplaced
        << st.stats.sum.num_objects_unfound
        << st.stats.sum.num_bytes
        << st.log_size
        << st.ondisk_log_size
        << pg_state_string(st.state)
        << st.last_change
        << st.version
        << reported.str()
        << st.up
        << st.up_primary
        << st.acting
        << st.acting_primary
        << st.last_scrub
        << st.last_scrub_stamp
        << st.last_deep_scrub
        << st.last_deep_scrub_stamp
        << TextTable::endrow;
  }

  ss << tab;
}

int64_t PGMap::get_rule_avail(const OSDMap& osdmap, int ruleno) const
{
  map<int,float> wm;
  int r = osdmap.crush->get_rule_weight_osd_map(ruleno, &wm);
  if (r < 0) {
    return r;
  }
  if (wm.empty()) {
    return 0;
  }

  float fratio;
  if (osdmap.test_flag(CEPH_OSDMAP_REQUIRE_LUMINOUS) && osdmap.get_full_ratio() > 0) {
    fratio = osdmap.get_full_ratio();
  } else if (full_ratio > 0) {
    fratio = full_ratio;
  } else {
    // this shouldn't really happen
    fratio = g_conf->mon_osd_full_ratio;
    if (fratio > 1.0) fratio /= 100;
  }

  int64_t min = -1;
  for (map<int,float>::iterator p = wm.begin(); p != wm.end(); ++p) {
    ceph::unordered_map<int32_t,osd_stat_t>::const_iterator osd_info =
      osd_stat.find(p->first);
    if (osd_info != osd_stat.end()) {
      if (osd_info->second.kb == 0 || p->second == 0) {
	// osd must be out, hence its stats have been zeroed
	// (unless we somehow managed to have a disk with size 0...)
	//
	// (p->second == 0), if osd weight is 0, no need to
	// calculate proj below.
	continue;
      }
      double unusable = (double)osd_info->second.kb *
	(1.0 - fratio);
      double avail = MAX(0.0, (double)osd_info->second.kb_avail - unusable);
      avail *= 1024.0;
      int64_t proj = (int64_t)(avail / (double)p->second);
      if (min < 0 || proj < min) {
	min = proj;
      }
    } else {
      dout(0) << "Cannot get stat of OSD " << p->first << dendl;
    }
  }
  return min;
}

inline std::string percentify(const float& a) {
  std::stringstream ss;
  if (a < 0.01)
    ss << "0";
  else
    ss << std::fixed << std::setprecision(2) << a;
  return ss.str();
}

void PGMap::dump_pool_stats(const OSDMap &osd_map, stringstream *ss,
    Formatter *f, bool verbose) const
{
  TextTable tbl;

  if (f) {
    f->open_array_section("pools");
  } else {
    tbl.define_column("NAME", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("ID", TextTable::LEFT, TextTable::LEFT);
    if (verbose) {
      tbl.define_column("QUOTA OBJECTS", TextTable::LEFT, TextTable::LEFT);
      tbl.define_column("QUOTA BYTES", TextTable::LEFT, TextTable::LEFT);
    }

    tbl.define_column("USED", TextTable::LEFT, TextTable::RIGHT);
    tbl.define_column("%USED", TextTable::LEFT, TextTable::RIGHT);
    tbl.define_column("MAX AVAIL", TextTable::LEFT, TextTable::RIGHT);
    tbl.define_column("OBJECTS", TextTable::LEFT, TextTable::RIGHT);
    if (verbose) {
      tbl.define_column("DIRTY", TextTable::LEFT, TextTable::RIGHT);
      tbl.define_column("READ", TextTable::LEFT, TextTable::RIGHT);
      tbl.define_column("WRITE", TextTable::LEFT, TextTable::RIGHT);
      tbl.define_column("RAW USED", TextTable::LEFT, TextTable::RIGHT);
    }
  }

  map<int,uint64_t> avail_by_rule;
  for (map<int64_t,pg_pool_t>::const_iterator p = osd_map.get_pools().begin();
       p != osd_map.get_pools().end(); ++p) {
    int64_t pool_id = p->first;
    if ((pool_id < 0) || (pg_pool_sum.count(pool_id) == 0))
      continue;
    const string& pool_name = osd_map.get_pool_name(pool_id);
    const pool_stat_t &stat = pg_pool_sum.at(pool_id);

    const pg_pool_t *pool = osd_map.get_pg_pool(pool_id);
    int ruleno = osd_map.crush->find_rule(pool->get_crush_ruleset(),
                                         pool->get_type(),
                                         pool->get_size());
    int64_t avail;
    float raw_used_rate;
    if (avail_by_rule.count(ruleno) == 0) {
      avail = get_rule_avail(osd_map, ruleno);
      if (avail < 0)
	avail = 0;
      avail_by_rule[ruleno] = avail;
    } else {
      avail = avail_by_rule[ruleno];
    }
    switch (pool->get_type()) {
    case pg_pool_t::TYPE_REPLICATED:
      avail /= pool->get_size();
      raw_used_rate = pool->get_size();
      break;
    case pg_pool_t::TYPE_ERASURE:
    {
      const map<string,string>& ecp =
        osd_map.get_erasure_code_profile(pool->erasure_code_profile);
      map<string,string>::const_iterator pm = ecp.find("m");
      map<string,string>::const_iterator pk = ecp.find("k");
      if (pm != ecp.end() && pk != ecp.end()) {
	int k = atoi(pk->second.c_str());
	int m = atoi(pm->second.c_str());
	avail = avail * k / (m + k);
	raw_used_rate = (float)(m + k) / k;
      } else {
	raw_used_rate = 0.0;
      }
    }
    break;
    default:
      assert(0 == "unrecognized pool type");
    }

    if (f) {
      f->open_object_section("pool");
      f->dump_string("name", pool_name);
      f->dump_int("id", pool_id);
      f->open_object_section("stats");
    } else {
      tbl << pool_name
          << pool_id;
      if (verbose) {
        if (pool->quota_max_objects == 0)
          tbl << "N/A";
        else
          tbl << si_t(pool->quota_max_objects);

        if (pool->quota_max_bytes == 0)
          tbl << "N/A";
        else
          tbl << si_t(pool->quota_max_bytes);
      }

    }
    dump_object_stat_sum(tbl, f, stat.stats.sum, avail, raw_used_rate, verbose, pool);
    if (f)
      f->close_section();  // stats
    else
      tbl << TextTable::endrow;

    if (f)
      f->close_section();  // pool
  }
  if (f)
    f->close_section();
  else {
    assert(ss != nullptr);
    *ss << "POOLS:\n";
    tbl.set_indent(4);
    *ss << tbl;
  }
}

void PGMap::dump_fs_stats(
    stringstream *ss, Formatter *f, bool verbose) const
{
  if (f) {
    f->open_object_section("stats");
    f->dump_int("total_bytes", osd_sum.kb * 1024ull);
    f->dump_int("total_used_bytes", osd_sum.kb_used * 1024ull);
    f->dump_int("total_avail_bytes", osd_sum.kb_avail * 1024ull);
    if (verbose) {
      f->dump_int("total_objects", pg_sum.stats.sum.num_objects);
    }
    f->close_section();
  } else {
    assert(ss != nullptr);
    TextTable tbl;
    tbl.define_column("SIZE", TextTable::LEFT, TextTable::RIGHT);
    tbl.define_column("AVAIL", TextTable::LEFT, TextTable::RIGHT);
    tbl.define_column("RAW USED", TextTable::LEFT, TextTable::RIGHT);
    tbl.define_column("%RAW USED", TextTable::LEFT, TextTable::RIGHT);
    if (verbose) {
      tbl.define_column("OBJECTS", TextTable::LEFT, TextTable::RIGHT);
    }
    tbl << stringify(si_t(osd_sum.kb*1024))
        << stringify(si_t(osd_sum.kb_avail*1024))
        << stringify(si_t(osd_sum.kb_used*1024));
    float used = 0.0;
    if (osd_sum.kb > 0) {
      used = ((float)osd_sum.kb_used / osd_sum.kb);
    }
    tbl << percentify(used*100);
    if (verbose) {
      tbl << stringify(si_t(pg_sum.stats.sum.num_objects));
    }
    tbl << TextTable::endrow;

    *ss << "GLOBAL:\n";
    tbl.set_indent(4);
    *ss << tbl;
  }
}

void PGMap::dump_object_stat_sum(TextTable &tbl, Formatter *f,
                                 const object_stat_sum_t &sum, uint64_t avail,
                                 float raw_used_rate, bool verbose,
                                 const pg_pool_t *pool)
{
  float curr_object_copies_rate = 0.0;
  if (sum.num_object_copies > 0)
    curr_object_copies_rate = (float)(sum.num_object_copies - sum.num_objects_degraded) / sum.num_object_copies;

  if (f) {
    f->dump_int("kb_used", SHIFT_ROUND_UP(sum.num_bytes, 10));
    f->dump_int("bytes_used", sum.num_bytes);
    f->dump_unsigned("max_avail", avail);
    f->dump_int("objects", sum.num_objects);
    if (verbose) {
      f->dump_int("quota_objects", pool->quota_max_objects);
      f->dump_int("quota_bytes", pool->quota_max_bytes);
      f->dump_int("dirty", sum.num_objects_dirty);
      f->dump_int("rd", sum.num_rd);
      f->dump_int("rd_bytes", sum.num_rd_kb * 1024ull);
      f->dump_int("wr", sum.num_wr);
      f->dump_int("wr_bytes", sum.num_wr_kb * 1024ull);
      f->dump_int("raw_bytes_used", sum.num_bytes * raw_used_rate * curr_object_copies_rate);
    }
  } else {
    tbl << stringify(si_t(sum.num_bytes));
    float used = 0.0;
    if (avail) {
      used = sum.num_bytes * curr_object_copies_rate;
      used /= used + avail;
    } else if (sum.num_bytes) {
      used = 1.0;
    }
    tbl << percentify(used*100);
    tbl << si_t(avail);
    tbl << sum.num_objects;
    if (verbose) {
      tbl << stringify(si_t(sum.num_objects_dirty))
          << stringify(si_t(sum.num_rd))
          << stringify(si_t(sum.num_wr))
          << stringify(si_t(sum.num_bytes * raw_used_rate * curr_object_copies_rate));
    }
  }
}


int process_pg_map_command(
  const string& orig_prefix,
  const map<string,cmd_vartype>& orig_cmdmap,
  const PGMap& pg_map,
  const OSDMap& osdmap,
  Formatter *f,
  stringstream *ss,
  bufferlist *odata)
{
  string prefix = orig_prefix;
  map<string,cmd_vartype> cmdmap = orig_cmdmap;

  // perhaps these would be better in the parsing, but it's weird
  bool primary = false;
  if (prefix == "pg dump_json") {
    vector<string> v;
    v.push_back(string("all"));
    cmd_putval(g_ceph_context, cmdmap, "format", string("json"));
    cmd_putval(g_ceph_context, cmdmap, "dumpcontents", v);
    prefix = "pg dump";
  } else if (prefix == "pg dump_pools_json") {
    vector<string> v;
    v.push_back(string("pools"));
    cmd_putval(g_ceph_context, cmdmap, "format", string("json"));
    cmd_putval(g_ceph_context, cmdmap, "dumpcontents", v);
    prefix = "pg dump";
  } else if (prefix == "pg ls-by-primary") {
    primary = true;
    prefix = "pg ls";
  } else if (prefix == "pg ls-by-osd") {
    prefix = "pg ls";
  } else if (prefix == "pg ls-by-pool") {
    prefix = "pg ls";
    string poolstr;
    cmd_getval(g_ceph_context, cmdmap, "poolstr", poolstr);
    int64_t pool = osdmap.lookup_pg_pool_name(poolstr.c_str());
    if (pool < 0) {
      *ss << "pool " << poolstr << " does not exist";
      return -ENOENT;
    }
    cmd_putval(g_ceph_context, cmdmap, "pool", pool);
  }

  int r = 0;
  stringstream ds;
  if (prefix == "pg stat") {
    if (f) {
      f->open_object_section("pg_summary");
      pg_map.print_oneline_summary(f, NULL);
      f->close_section();
      f->flush(ds);
    } else {
      ds << pg_map;
    }
    odata->append(ds);
    return 0;
  }

  if (prefix == "pg getmap") {
    pg_map.encode(*odata);
    *ss << "got pgmap version " << pg_map.version;
    return 0;
  }

  if (prefix == "pg dump") {
    string val;
    vector<string> dumpcontents;
    set<string> what;
    if (cmd_getval(g_ceph_context, cmdmap, "dumpcontents", dumpcontents)) {
      copy(dumpcontents.begin(), dumpcontents.end(),
           inserter(what, what.end()));
    }
    if (what.empty())
      what.insert("all");
    if (f) {
      if (what.count("all")) {
	f->open_object_section("pg_map");
	pg_map.dump(f);
	f->close_section();
      } else if (what.count("summary") || what.count("sum")) {
	f->open_object_section("pg_map");
	pg_map.dump_basic(f);
	f->close_section();
      } else {
	if (what.count("pools")) {
	  pg_map.dump_pool_stats(f);
	}
	if (what.count("osds")) {
	  pg_map.dump_osd_stats(f);
	}
	if (what.count("pgs")) {
	  pg_map.dump_pg_stats(f, false);
	}
	if (what.count("pgs_brief")) {
	  pg_map.dump_pg_stats(f, true);
	}
	if (what.count("delta")) {
	  f->open_object_section("delta");
	  pg_map.dump_delta(f);
	  f->close_section();
	}
      }
      f->flush(*odata);
    } else {
      if (what.count("all")) {
	pg_map.dump(ds);
      } else if (what.count("summary") || what.count("sum")) {
	pg_map.dump_basic(ds);
	pg_map.dump_pg_sum_stats(ds, true);
	pg_map.dump_osd_sum_stats(ds);
      } else {
	if (what.count("pgs_brief")) {
	  pg_map.dump_pg_stats(ds, true);
	}
	bool header = true;
	if (what.count("pgs")) {
	  pg_map.dump_pg_stats(ds, false);
	  header = false;
	}
	if (what.count("pools")) {
	  pg_map.dump_pool_stats(ds, header);
	}
	if (what.count("osds")) {
	  pg_map.dump_osd_stats(ds);
	}
      }
      odata->append(ds);
    }
    *ss << "dumped " << what;
    return 0;
  }

  if (prefix == "pg ls") {
    int64_t osd = -1;
    int64_t pool = -1;
    vector<string>states;
    set<pg_t> pgs;
    cmd_getval(g_ceph_context, cmdmap, "pool", pool);
    cmd_getval(g_ceph_context, cmdmap, "osd", osd);
    cmd_getval(g_ceph_context, cmdmap, "states", states);
    if (pool >= 0 && !osdmap.have_pg_pool(pool)) {
      *ss << "pool " << pool << " does not exist";
      return -ENOENT;
    }
    if (osd >= 0 && !osdmap.is_up(osd)) {
      *ss << "osd " << osd << " is not up";
      return -EAGAIN;
    }
    if (states.empty())
      states.push_back("all");

    uint32_t state = 0;

    while (!states.empty()) {
      string state_str = states.back();

      if (state_str == "all") {
        state = -1;
        break;
      } else {
        int filter = pg_string_state(state_str);
        assert(filter != -1);
        state |= filter;
      }

      states.pop_back();
    }

    pg_map.get_filtered_pg_stats(state, pool, osd, primary, pgs);

    if (f && !pgs.empty()) {
      pg_map.dump_filtered_pg_stats(f, pgs);
      f->flush(*odata);
    } else if (!pgs.empty()) {
      pg_map.dump_filtered_pg_stats(ds, pgs);
      odata->append(ds);
    }
    return 0;
  }

  if (prefix == "pg dump_stuck") {
    vector<string> stuckop_vec;
    cmd_getval(g_ceph_context, cmdmap, "stuckops", stuckop_vec);
    if (stuckop_vec.empty())
      stuckop_vec.push_back("unclean");
    int64_t threshold;
    cmd_getval(g_ceph_context, cmdmap, "threshold", threshold,
               int64_t(g_conf->mon_pg_stuck_threshold));

    r = pg_map.dump_stuck_pg_stats(ds, f, (int)threshold, stuckop_vec);
    odata->append(ds);
    if (r < 0)
      *ss << "failed";
    else
      *ss << "ok";
    return 0;
  }

  if (prefix == "pg debug") {
    string debugop;
    cmd_getval(g_ceph_context, cmdmap, "debugop", debugop,
	       string("unfound_objects_exist"));
    if (debugop == "unfound_objects_exist") {
      bool unfound_objects_exist = false;
      for (const auto& p : pg_map.pg_stat) {
	if (p.second.stats.sum.num_objects_unfound > 0) {
	  unfound_objects_exist = true;
	  break;
	}
      }
      if (unfound_objects_exist)
	ds << "TRUE";
      else
	ds << "FALSE";
      odata->append(ds);
      return 0;
    }
    if (debugop == "degraded_pgs_exist") {
      bool degraded_pgs_exist = false;
      for (const auto& p : pg_map.pg_stat) {
	if (p.second.stats.sum.num_objects_degraded > 0) {
	  degraded_pgs_exist = true;
	  break;
	}
      }
      if (degraded_pgs_exist)
	ds << "TRUE";
      else
	ds << "FALSE";
      odata->append(ds);
      return 0;
    }
  }

  if (prefix == "osd perf") {
    if (f) {
      f->open_object_section("osdstats");
      pg_map.dump_osd_perf_stats(f);
      f->close_section();
      f->flush(ds);
    } else {
      pg_map.print_osd_perf_stats(&ds);
    }
    odata->append(ds);
    return 0;
  }

  if (prefix == "osd blocked-by") {
    if (f) {
      f->open_object_section("osd_blocked_by");
      pg_map.dump_osd_blocked_by_stats(f);
      f->close_section();
      f->flush(ds);
    } else {
      pg_map.print_osd_blocked_by_stats(&ds);
    }
    odata->append(ds);
    return 0;
  }

  if (prefix == "osd pool stats") {
    string pool_name;
    cmd_getval(g_ceph_context, cmdmap, "name", pool_name);

    int64_t poolid = -ENOENT;
    bool one_pool = false;
    if (!pool_name.empty()) {
      poolid = osdmap.lookup_pg_pool_name(pool_name);
      if (poolid < 0) {
        assert(poolid == -ENOENT);
        *ss << "unrecognized pool '" << pool_name << "'";
        return -ENOENT;
      }
      one_pool = true;
    }

    stringstream rs;

    if (f)
      f->open_array_section("pool_stats");
    else {
      if (osdmap.get_pools().empty()) {
        *ss << "there are no pools!";
	goto stats_out;
      }
    }

    for (auto& p : osdmap.get_pools()) {
      if (!one_pool)
        poolid = p.first;

      pool_name = osdmap.get_pool_name(poolid);

      if (f) {
        f->open_object_section("pool");
        f->dump_string("pool_name", pool_name.c_str());
        f->dump_int("pool_id", poolid);
        f->open_object_section("recovery");
      }

      list<string> sl;
      stringstream tss;
      pg_map.pool_recovery_summary(f, &sl, poolid);
      if (!f && !sl.empty()) {
	for (auto& p : sl)
	  tss << "  " << p << "\n";
      }

      if (f) {
        f->close_section();
        f->open_object_section("recovery_rate");
      }

      ostringstream rss;
      pg_map.pool_recovery_rate_summary(f, &rss, poolid);
      if (!f && !rss.str().empty())
        tss << "  recovery io " << rss.str() << "\n";

      if (f) {
        f->close_section();
        f->open_object_section("client_io_rate");
      }
      rss.clear();
      rss.str("");

      pg_map.pool_client_io_rate_summary(f, &rss, poolid);
      if (!f && !rss.str().empty())
        tss << "  client io " << rss.str() << "\n";

      // dump cache tier IO rate for cache pool
      const pg_pool_t *pool = osdmap.get_pg_pool(poolid);
      if (pool->is_tier()) {
        if (f) {
          f->close_section();
          f->open_object_section("cache_io_rate");
        }
        rss.clear();
        rss.str("");

        pg_map.pool_cache_io_rate_summary(f, &rss, poolid);
        if (!f && !rss.str().empty())
          tss << "  cache tier io " << rss.str() << "\n";
      }
      if (f) {
        f->close_section();
        f->close_section();
      } else {
        rs << "pool " << pool_name << " id " << poolid << "\n";
        if (!tss.str().empty())
          rs << tss.str() << "\n";
        else
          rs << "  nothing is going on\n\n";
      }
      if (one_pool)
        break;
    }

stats_out:
    if (f) {
      f->close_section();
      f->flush(ds);
      odata->append(ds);
    } else {
      odata->append(rs.str());
    }
    return 0;
  }

  return -EOPNOTSUPP;
}

void PGMapUpdater::check_osd_map(const OSDMap::Incremental &osd_inc,
                                 std::set<int> *need_check_down_pg_osds,
                                 std::map<int,utime_t> *last_osd_report,
                                 PGMap *pg_map,
                                 PGMap::Incremental *pending_inc)
{
  for (const auto &p : osd_inc.new_weight) {
    if (p.second == CEPH_OSD_OUT) {
      dout(10) << __func__ << "  osd." << p.first << " went OUT" << dendl;
      pending_inc->stat_osd_out(p.first);
    }
  }

  // this is conservative: we want to know if any osds (maybe) got marked down.
  for (const auto &p : osd_inc.new_state) {
    if (p.second & CEPH_OSD_UP) {   // true if marked up OR down,
                                     // but we're too lazy to check
                                     // which
      need_check_down_pg_osds->insert(p.first);

      // clear out the last_osd_report for this OSD
      map<int, utime_t>::iterator report = last_osd_report->find(p.first);
      if (report != last_osd_report->end()) {
        last_osd_report->erase(report);
      }

      // clear out osd_stat slow request histogram
      dout(20) << __func__ << " clearing osd." << p.first
               << " request histogram" << dendl;
      pending_inc->stat_osd_down_up(p.first, *pg_map);
    }

    if (p.second & CEPH_OSD_EXISTS) {
      // whether it was created *or* destroyed, we can safely drop
      // it's osd_stat_t record.
      dout(10) << __func__ << "  osd." << p.first
               << " created or destroyed" << dendl;
      pending_inc->rm_stat(p.first);

      // and adjust full, nearfull set
      pg_map->nearfull_osds.erase(p.first);
      pg_map->full_osds.erase(p.first);
    }
  }
}

void PGMapUpdater::register_pg(
    const OSDMap &osd_map,
    pg_t pgid, epoch_t epoch,
    bool new_pool,
    const PGMap &pg_map,
    PGMap::Incremental *pending_inc)
{
  pg_t parent;
  int split_bits = 0;
  auto parent_stat = pg_map.pg_stat.end();
  if (!new_pool) {
    parent = pgid;
    while (1) {
      // remove most significant bit
      int msb = cbits(parent.ps());
      if (!msb)
	break;
      parent.set_ps(parent.ps() & ~(1<<(msb-1)));
      split_bits++;
      dout(30) << " is " << pgid << " parent " << parent << " ?" << dendl;
      parent_stat = pg_map.pg_stat.find(parent);
      if (parent_stat != pg_map.pg_stat.end() &&
          parent_stat->second.state != PG_STATE_CREATING) {
	dout(10) << "  parent is " << parent << dendl;
	break;
      }
    }
  }

  pg_stat_t &stats = pending_inc->pg_stat_updates[pgid];
  stats.state = PG_STATE_CREATING;
  stats.created = epoch;
  stats.parent = parent;
  stats.parent_split_bits = split_bits;
  stats.mapping_epoch = epoch;

  if (parent_stat != pg_map.pg_stat.end()) {
    const pg_stat_t &ps = parent_stat->second;
    stats.last_fresh = ps.last_fresh;
    stats.last_active = ps.last_active;
    stats.last_change = ps.last_change;
    stats.last_peered = ps.last_peered;
    stats.last_clean = ps.last_clean;
    stats.last_unstale = ps.last_unstale;
    stats.last_undegraded = ps.last_undegraded;
    stats.last_fullsized = ps.last_fullsized;
    stats.last_scrub_stamp = ps.last_scrub_stamp;
    stats.last_deep_scrub_stamp = ps.last_deep_scrub_stamp;
    stats.last_clean_scrub_stamp = ps.last_clean_scrub_stamp;
  } else {
    utime_t now = osd_map.get_modified();
    stats.last_fresh = now;
    stats.last_active = now;
    stats.last_change = now;
    stats.last_peered = now;
    stats.last_clean = now;
    stats.last_unstale = now;
    stats.last_undegraded = now;
    stats.last_fullsized = now;
    stats.last_scrub_stamp = now;
    stats.last_deep_scrub_stamp = now;
    stats.last_clean_scrub_stamp = now;
  }

  osd_map.pg_to_up_acting_osds(
    pgid,
    &stats.up,
    &stats.up_primary,
    &stats.acting,
    &stats.acting_primary);

  if (split_bits == 0) {
    dout(10) << __func__ << "  will create " << pgid
             << " primary " << stats.acting_primary
             << " acting " << stats.acting
             << dendl;
  } else {
    dout(10) << __func__ << "  will create " << pgid
             << " primary " << stats.acting_primary
             << " acting " << stats.acting
             << " parent " << parent
             << " by " << split_bits << " bits"
             << dendl;
  }
}

void PGMapUpdater::register_new_pgs(
    const OSDMap &osd_map,
    const PGMap &pg_map,
    PGMap::Incremental *pending_inc)
{
  epoch_t epoch = osd_map.get_epoch();
  dout(10) << __func__ << " checking pg pools for osdmap epoch " << epoch
           << ", last_pg_scan " << pg_map.last_pg_scan << dendl;

  int created = 0;
  const auto &pools = osd_map.get_pools();

  for (const auto &p : pools) {
    int64_t poolid = p.first;
    const pg_pool_t &pool = p.second;
    int ruleno = osd_map.crush->find_rule(pool.get_crush_ruleset(),
                                          pool.get_type(), pool.get_size());
    if (ruleno < 0 || !osd_map.crush->rule_exists(ruleno))
      continue;

    if (pool.get_last_change() <= pg_map.last_pg_scan ||
        pool.get_last_change() <= pending_inc->pg_scan) {
      dout(10) << " no change in pool " << poolid << " " << pool << dendl;
      continue;
    }

    dout(10) << __func__ << " scanning pool " << poolid
             << " " << pool << dendl;

    // first pgs in this pool
    bool new_pool = pg_map.pg_pool_sum.count(poolid) == 0;

    for (ps_t ps = 0; ps < pool.get_pg_num(); ps++) {
      pg_t pgid(ps, poolid, -1);
      if (pg_map.pg_stat.count(pgid)) {
	dout(20) << "register_new_pgs  have " << pgid << dendl;
	continue;
      }
      created++;
      register_pg(osd_map, pgid, pool.get_last_change(), new_pool,
                  pg_map, pending_inc);
    }
  }

  int removed = 0;
  for (const auto &p : pg_map.creating_pgs) {
    if (p.preferred() >= 0) {
      dout(20) << " removing creating_pg " << p
               << " because it is localized and obsolete" << dendl;
      pending_inc->pg_remove.insert(p);
      ++removed;
    } else if (!osd_map.have_pg_pool(p.pool())) {
      dout(20) << " removing creating_pg " << p
               << " because containing pool deleted" << dendl;
      pending_inc->pg_remove.insert(p);
      ++removed;
    }
  }

  // deleted pools?
  for (const auto &p : pg_map.pg_stat) {
    if (!osd_map.have_pg_pool(p.first.pool())) {
      dout(20) << " removing pg_stat " << p.first << " because "
               << "containing pool deleted" << dendl;
      pending_inc->pg_remove.insert(p.first);
      ++removed;
    } else if (p.first.preferred() >= 0) {
      dout(20) << " removing localized pg " << p.first << dendl;
      pending_inc->pg_remove.insert(p.first);
      ++removed;
    }
  }

  // we don't want to redo this work if we can avoid it.
  pending_inc->pg_scan = epoch;

  dout(10) << "register_new_pgs registered " << created << " new pgs, removed "
           << removed << " uncreated pgs" << dendl;
}


void PGMapUpdater::update_creating_pgs(
    const OSDMap &osd_map,
    const PGMap &pg_map,
    PGMap::Incremental *pending_inc)
{
  dout(10) << __func__ << " to " << pg_map.creating_pgs.size()
           << " pgs, osdmap epoch " << osd_map.get_epoch()
           << dendl;

  unsigned changed = 0;
  for (set<pg_t>::const_iterator p = pg_map.creating_pgs.begin();
       p != pg_map.creating_pgs.end();
       ++p) {
    pg_t pgid = *p;
    pg_t on = pgid;
    ceph::unordered_map<pg_t,pg_stat_t>::const_iterator q =
      pg_map.pg_stat.find(pgid);
    assert(q != pg_map.pg_stat.end());
    const pg_stat_t *s = &q->second;

    if (s->parent_split_bits)
      on = s->parent;

    vector<int> up, acting;
    int up_primary, acting_primary;
    osd_map.pg_to_up_acting_osds(
      on,
      &up,
      &up_primary,
      &acting,
      &acting_primary);

    if (up != s->up ||
        up_primary != s->up_primary ||
        acting !=  s->acting ||
        acting_primary != s->acting_primary) {
      pg_stat_t *ns = &pending_inc->pg_stat_updates[pgid];
      if (osd_map.get_epoch() > ns->reported_epoch) {
	dout(20) << __func__ << "  " << pgid << " "
		 << " acting_primary: " << s->acting_primary
		 << " -> " << acting_primary
		 << " acting: " << s->acting << " -> " << acting
		 << " up_primary: " << s->up_primary << " -> " << up_primary
		 << " up: " << s->up << " -> " << up
		 << dendl;

	// only initialize if it wasn't already a pending update
	if (ns->reported_epoch == 0)
	  *ns = *s;

	// note epoch if the target of the create message changed
	if (acting_primary != ns->acting_primary)
	  ns->mapping_epoch = osd_map.get_epoch();

	ns->up = up;
	ns->up_primary = up_primary;
	ns->acting = acting;
	ns->acting_primary = acting_primary;

	++changed;
      } else {
	dout(20) << __func__ << "  " << pgid << " has pending update from newer"
		 << " epoch " << ns->reported_epoch
		 << dendl;
      }
    }
  }
  if (changed) {
    dout(10) << __func__ << " " << changed << " pgs changed primary" << dendl;
  }
}

static void _try_mark_pg_stale(
  const OSDMap& osdmap,
  pg_t pgid,
  const pg_stat_t& cur,
  PGMap::Incremental *pending_inc)
{
  if ((cur.state & PG_STATE_STALE) == 0 &&
      cur.acting_primary != -1 &&
      osdmap.is_down(cur.acting_primary)) {
    pg_stat_t *newstat;
    auto q = pending_inc->pg_stat_updates.find(pgid);
    if (q != pending_inc->pg_stat_updates.end()) {
      if ((q->second.acting_primary == cur.acting_primary) ||
	  ((q->second.state & PG_STATE_STALE) == 0 &&
	   q->second.acting_primary != -1 &&
	   osdmap.is_down(q->second.acting_primary))) {
	newstat = &q->second;
      } else {
	// pending update is no longer down or already stale
	return;
      }
    } else {
      newstat = &pending_inc->pg_stat_updates[pgid];
      *newstat = cur;
    }
    dout(10) << __func__ << " marking pg " << pgid
	     << " stale (acting_primary " << newstat->acting_primary
	     << ")" << dendl;
    newstat->state |= PG_STATE_STALE;
    newstat->last_unstale = ceph_clock_now();
  }
}

void PGMapUpdater::check_down_pgs(
    const OSDMap &osdmap,
    const PGMap &pg_map,
    bool check_all,
    const set<int>& need_check_down_pg_osds,
    PGMap::Incremental *pending_inc)
{
  // if a large number of osds changed state, just iterate over the whole
  // pg map.
  if (need_check_down_pg_osds.size() > (unsigned)osdmap.get_num_osds() *
      g_conf->mon_pg_check_down_all_threshold) {
    check_all = true;
  }

  if (check_all) {
    for (const auto& p : pg_map.pg_stat) {
      _try_mark_pg_stale(osdmap, p.first, p.second, pending_inc);
    }
  } else {
    for (auto osd : need_check_down_pg_osds) {
      if (osdmap.is_down(osd)) {
	auto p = pg_map.pg_by_osd.find(osd);
	if (p == pg_map.pg_by_osd.end()) {
	  continue;
	}
	for (auto pgid : p->second) {
	  const pg_stat_t &stat = pg_map.pg_stat.at(pgid);
	  assert(stat.acting_primary == osd);
	  _try_mark_pg_stale(osdmap, pgid, stat, pending_inc);
	}
      }
    }
  }
}

int reweight::by_utilization(
    const OSDMap &osdmap,
    const PGMap &pgm,
    int oload,
    double max_changef,
    int max_osds,
    bool by_pg, const set<int64_t> *pools,
    bool no_increasing,
    map<int32_t, uint32_t>* new_weights,
    std::stringstream *ss,
    std::string *out_str,
    Formatter *f)
{
  if (oload <= 100) {
    *ss << "You must give a percentage higher than 100. "
      "The reweighting threshold will be calculated as <average-utilization> "
      "times <input-percentage>. For example, an argument of 200 would "
      "reweight OSDs which are twice as utilized as the average OSD.\n";
    return -EINVAL;
  }

  vector<int> pgs_by_osd(osdmap.get_max_osd());

  // Avoid putting a small number (or 0) in the denominator when calculating
  // average_util
  double average_util;
  if (by_pg) {
    // by pg mapping
    double weight_sum = 0.0;      // sum up the crush weights
    unsigned num_pg_copies = 0;
    int num_osds = 0;
    for (const auto& pg : pgm.pg_stat) {
      if (pools && pools->count(pg.first.pool()) == 0)
	continue;
      for (const auto acting : pg.second.acting) {
	if (acting >= (int)pgs_by_osd.size())
	  pgs_by_osd.resize(acting);
	if (pgs_by_osd[acting] == 0) {
          if (osdmap.crush->get_item_weightf(acting) <= 0) {
            //skip if we currently can not identify item
            continue;
          }
	  weight_sum += osdmap.crush->get_item_weightf(acting);
	  ++num_osds;
	}
	++pgs_by_osd[acting];
	++num_pg_copies;
      }
    }

    if (!num_osds || (num_pg_copies / num_osds < g_conf->mon_reweight_min_pgs_per_osd)) {
      *ss << "Refusing to reweight: we only have " << num_pg_copies
	  << " PGs across " << num_osds << " osds!\n";
      return -EDOM;
    }

    average_util = (double)num_pg_copies / weight_sum;
  } else {
    // by osd utilization
    int num_osd = MAX(1, pgm.osd_stat.size());
    if ((uint64_t)pgm.osd_sum.kb * 1024 / num_osd
	< g_conf->mon_reweight_min_bytes_per_osd) {
      *ss << "Refusing to reweight: we only have " << pgm.osd_sum.kb
	  << " kb across all osds!\n";
      return -EDOM;
    }
    if ((uint64_t)pgm.osd_sum.kb_used * 1024 / num_osd
	< g_conf->mon_reweight_min_bytes_per_osd) {
      *ss << "Refusing to reweight: we only have " << pgm.osd_sum.kb_used
	  << " kb used across all osds!\n";
      return -EDOM;
    }

    average_util = (double)pgm.osd_sum.kb_used / (double)pgm.osd_sum.kb;
  }

  // adjust down only if we are above the threshold
  const double overload_util = average_util * (double)oload / 100.0;

  // but aggressively adjust weights up whenever possible.
  const double underload_util = average_util;

  const unsigned max_change = (unsigned)(max_changef * (double)0x10000);

  ostringstream oss;
  if (f) {
    f->open_object_section("reweight_by_utilization");
    f->dump_int("overload_min", oload);
    f->dump_float("max_change", max_changef);
    f->dump_int("max_change_osds", max_osds);
    f->dump_float("average_utilization", average_util);
    f->dump_float("overload_utilization", overload_util);
  } else {
    oss << "oload " << oload << "\n";
    oss << "max_change " << max_changef << "\n";
    oss << "max_change_osds " << max_osds << "\n";
    oss.precision(4);
    oss << "average_utilization " << std::fixed << average_util << "\n";
    oss << "overload_utilization " << overload_util << "\n";
  }
  int num_changed = 0;

  // precompute util for each OSD
  std::vector<std::pair<int, float> > util_by_osd;
  for (const auto& p : pgm.osd_stat) {
    std::pair<int, float> osd_util;
    osd_util.first = p.first;
    if (by_pg) {
      if (p.first >= (int)pgs_by_osd.size() ||
        pgs_by_osd[p.first] == 0) {
        // skip if this OSD does not contain any pg
        // belonging to the specified pool(s).
        continue;
      }

      if (osdmap.crush->get_item_weightf(p.first) <= 0) {
        // skip if we are unable to locate item.
        continue;
      }

      osd_util.second = pgs_by_osd[p.first] / osdmap.crush->get_item_weightf(p.first);
    } else {
      osd_util.second = (double)p.second.kb_used / (double)p.second.kb;
    }
    util_by_osd.push_back(osd_util);
  }

  // sort by absolute deviation from the mean utilization,
  // in descending order.
  std::sort(util_by_osd.begin(), util_by_osd.end(),
    [average_util](std::pair<int, float> l, std::pair<int, float> r) {
      return abs(l.second - average_util) > abs(r.second - average_util);
    }
  );

  if (f)
    f->open_array_section("reweights");

  for (const auto& p : util_by_osd) {
    unsigned weight = osdmap.get_weight(p.first);
    if (weight == 0) {
      // skip if OSD is currently out
      continue;
    }
    float util = p.second;

    if (util >= overload_util) {
      // Assign a lower weight to overloaded OSDs. The current weight
      // is a factor to take into account the original weights,
      // to represent e.g. differing storage capacities
      unsigned new_weight = (unsigned)((average_util / util) * (float)weight);
      if (weight > max_change)
	new_weight = MAX(new_weight, weight - max_change);
      new_weights->insert({p.first, new_weight});
      if (f) {
	f->open_object_section("osd");
	f->dump_int("osd", p.first);
	f->dump_float("weight", (float)weight / (float)0x10000);
	f->dump_float("new_weight", (float)new_weight / (float)0x10000);
	f->close_section();
      } else {
        oss << "osd." << p.first << " weight "
            << (float)weight / (float)0x10000 << " -> "
            << (float)new_weight / (float)0x10000 << "\n";
      }
      if (++num_changed >= max_osds)
	break;
    }
    if (!no_increasing && util <= underload_util) {
      // assign a higher weight.. if we can.
      unsigned new_weight = (unsigned)((average_util / util) * (float)weight);
      new_weight = MIN(new_weight, weight + max_change);
      if (new_weight > 0x10000)
	new_weight = 0x10000;
      if (new_weight > weight) {
	new_weights->insert({p.first, new_weight});
        oss << "osd." << p.first << " weight "
            << (float)weight / (float)0x10000 << " -> "
            << (float)new_weight / (float)0x10000 << "\n";
	if (++num_changed >= max_osds)
	  break;
      }
    }
  }
  if (f) {
    f->close_section();
  }

  OSDMap newmap;
  newmap.deepish_copy_from(osdmap);
  OSDMap::Incremental newinc;
  newinc.fsid = newmap.get_fsid();
  newinc.epoch = newmap.get_epoch() + 1;
  newinc.new_weight = *new_weights;
  newmap.apply_incremental(newinc);

  osdmap.summarize_mapping_stats(&newmap, pools, out_str, f);

  if (f) {
    f->close_section();
  } else {
    *out_str += "\n";
    *out_str += oss.str();
  }
  return num_changed;
}
