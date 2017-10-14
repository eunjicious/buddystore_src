// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "OSDMap.h"
#include <algorithm>
#include "common/config.h"
#include "common/Formatter.h"
#include "common/TextTable.h"
#include "include/ceph_features.h"
#include "include/str_map.h"

#include "common/code_environment.h"

#include "crush/CrushTreeDumper.h"
#include "common/Clock.h"
 
#define dout_subsys ceph_subsys_osd

// ----------------------------------
// osd_info_t

void osd_info_t::dump(Formatter *f) const
{
  f->dump_int("last_clean_begin", last_clean_begin);
  f->dump_int("last_clean_end", last_clean_end);
  f->dump_int("up_from", up_from);
  f->dump_int("up_thru", up_thru);
  f->dump_int("down_at", down_at);
  f->dump_int("lost_at", lost_at);
}

void osd_info_t::encode(bufferlist& bl) const
{
  __u8 struct_v = 1;
  ::encode(struct_v, bl);
  ::encode(last_clean_begin, bl);
  ::encode(last_clean_end, bl);
  ::encode(up_from, bl);
  ::encode(up_thru, bl);
  ::encode(down_at, bl);
  ::encode(lost_at, bl);
}

void osd_info_t::decode(bufferlist::iterator& bl)
{
  __u8 struct_v;
  ::decode(struct_v, bl);
  ::decode(last_clean_begin, bl);
  ::decode(last_clean_end, bl);
  ::decode(up_from, bl);
  ::decode(up_thru, bl);
  ::decode(down_at, bl);
  ::decode(lost_at, bl);
}

void osd_info_t::generate_test_instances(list<osd_info_t*>& o)
{
  o.push_back(new osd_info_t);
  o.push_back(new osd_info_t);
  o.back()->last_clean_begin = 1;
  o.back()->last_clean_end = 2;
  o.back()->up_from = 30;
  o.back()->up_thru = 40;
  o.back()->down_at = 5;
  o.back()->lost_at = 6;
}

ostream& operator<<(ostream& out, const osd_info_t& info)
{
  out << "up_from " << info.up_from
      << " up_thru " << info.up_thru
      << " down_at " << info.down_at
      << " last_clean_interval [" << info.last_clean_begin << "," << info.last_clean_end << ")";
  if (info.lost_at)
    out << " lost_at " << info.lost_at;
  return out;
}

// ----------------------------------
// osd_xinfo_t

void osd_xinfo_t::dump(Formatter *f) const
{
  f->dump_stream("down_stamp") << down_stamp;
  f->dump_float("laggy_probability", laggy_probability);
  f->dump_int("laggy_interval", laggy_interval);
  f->dump_int("features", features);
  f->dump_unsigned("old_weight", old_weight);
}

void osd_xinfo_t::encode(bufferlist& bl) const
{
  ENCODE_START(3, 1, bl);
  ::encode(down_stamp, bl);
  __u32 lp = laggy_probability * 0xfffffffful;
  ::encode(lp, bl);
  ::encode(laggy_interval, bl);
  ::encode(features, bl);
  ::encode(old_weight, bl);
  ENCODE_FINISH(bl);
}

void osd_xinfo_t::decode(bufferlist::iterator& bl)
{
  DECODE_START(3, bl);
  ::decode(down_stamp, bl);
  __u32 lp;
  ::decode(lp, bl);
  laggy_probability = (float)lp / (float)0xffffffff;
  ::decode(laggy_interval, bl);
  if (struct_v >= 2)
    ::decode(features, bl);
  else
    features = 0;
  if (struct_v >= 3)
    ::decode(old_weight, bl);
  else
    old_weight = 0;
  DECODE_FINISH(bl);
}

void osd_xinfo_t::generate_test_instances(list<osd_xinfo_t*>& o)
{
  o.push_back(new osd_xinfo_t);
  o.push_back(new osd_xinfo_t);
  o.back()->down_stamp = utime_t(2, 3);
  o.back()->laggy_probability = .123;
  o.back()->laggy_interval = 123456;
  o.back()->old_weight = 0x7fff;
}

ostream& operator<<(ostream& out, const osd_xinfo_t& xi)
{
  return out << "down_stamp " << xi.down_stamp
	     << " laggy_probability " << xi.laggy_probability
	     << " laggy_interval " << xi.laggy_interval
	     << " old_weight " << xi.old_weight;
}

// ----------------------------------
// OSDMap::Incremental

int OSDMap::Incremental::get_net_marked_out(const OSDMap *previous) const
{
  int n = 0;
  for (auto &weight : new_weight) {
    if (weight.second == CEPH_OSD_OUT && !previous->is_out(weight.first))
      n++;  // marked out
    else if (weight.second != CEPH_OSD_OUT && previous->is_out(weight.first))
      n--;  // marked in
  }
  return n;
}

int OSDMap::Incremental::get_net_marked_down(const OSDMap *previous) const
{
  int n = 0;
  for (auto &state : new_state) { // 
    if (state.second & CEPH_OSD_UP) {
      if (previous->is_up(state.first))
	n++;  // marked down
      else
	n--;  // marked up
    }
  }
  return n;
}

int OSDMap::Incremental::identify_osd(uuid_d u) const
{
  for (auto &uuid : new_uuid)
    if (uuid.second == u)
      return uuid.first;
  return -1;
}

int OSDMap::Incremental::propagate_snaps_to_tiers(CephContext *cct,
						  const OSDMap& osdmap)
{
  assert(epoch == osdmap.get_epoch() + 1);

  for (auto &new_pool : new_pools) {
    if (!new_pool.second.tiers.empty()) {
      pg_pool_t& base = new_pool.second;

      for (const auto &tier_pool : base.tiers) {
	const auto &r = new_pools.find(tier_pool);
	pg_pool_t *tier = 0;
	if (r == new_pools.end()) {
	  const pg_pool_t *orig = osdmap.get_pg_pool(tier_pool);
	  if (!orig) {
	    lderr(cct) << __func__ << " no pool " << tier_pool << dendl;
	    return -EIO;
	  }
	  tier = get_new_pool(tier_pool, orig);
	} else {
	  tier = &r->second;
	}
	if (tier->tier_of != new_pool.first) {
	  lderr(cct) << __func__ << " " << r->first << " tier_of != " << new_pool.first << dendl;
	  return -EIO;
	}

        ldout(cct, 10) << __func__ << " from " << new_pool.first << " to "
                       << tier_pool << dendl;
	tier->snap_seq = base.snap_seq;
	tier->snap_epoch = base.snap_epoch;
	tier->snaps = base.snaps;
	tier->removed_snaps = base.removed_snaps;
      }
    }
  }
  return 0;
}


bool OSDMap::subtree_is_down(int id, set<int> *down_cache) const
{
  if (id >= 0)
    return is_down(id);

  if (down_cache &&
      down_cache->count(id)) {
    return true;
  }

  list<int> children;
  crush->get_children(id, &children);
  for (const auto &child : children) {
    if (!subtree_is_down(child, down_cache)) {
      return false;
    }
  }
  if (down_cache) {
    down_cache->insert(id);
  }
  return true;
}

bool OSDMap::containing_subtree_is_down(CephContext *cct, int id, int subtree_type, set<int> *down_cache) const
{
  // use a stack-local down_cache if we didn't get one from the
  // caller.  then at least this particular call will avoid duplicated
  // work.
  set<int> local_down_cache;
  if (!down_cache) {
    down_cache = &local_down_cache;
  }

  int current = id;
  while (true) {
    int type;
    if (current >= 0) {
      type = 0;
    } else {
      type = crush->get_bucket_type(current);
    }
    assert(type >= 0);

    if (!subtree_is_down(current, down_cache)) {
      ldout(cct, 30) << "containing_subtree_is_down(" << id << ") = false" << dendl;
      return false;
    }

    // is this a big enough subtree to be marked as down?
    if (type >= subtree_type) {
      ldout(cct, 30) << "containing_subtree_is_down(" << id << ") = true ... " << type << " >= " << subtree_type << dendl;
      return true;
    }

    int r = crush->get_immediate_parent_id(current, &current);
    if (r < 0) {
      return false;
    }
  }
}

void OSDMap::Incremental::encode_client_old(bufferlist& bl) const
{
  __u16 v = 5;
  ::encode(v, bl);
  ::encode(fsid, bl);
  ::encode(epoch, bl);
  ::encode(modified, bl);
  int32_t new_t = new_pool_max;
  ::encode(new_t, bl);
  ::encode(new_flags, bl);
  ::encode(fullmap, bl);
  ::encode(crush, bl);

  ::encode(new_max_osd, bl);
  // for ::encode(new_pools, bl);
  __u32 n = new_pools.size();
  ::encode(n, bl);
  for (const auto &new_pool : new_pools) {
    n = new_pool.first;
    ::encode(n, bl);
    ::encode(new_pool.second, bl, 0);
  }
  // for ::encode(new_pool_names, bl);
  n = new_pool_names.size();
  ::encode(n, bl);

  for (const auto &new_pool_name : new_pool_names) {
    n = new_pool_name.first;
    ::encode(n, bl);
    ::encode(new_pool_name.second, bl);
  }
  // for ::encode(old_pools, bl);
  n = old_pools.size();
  ::encode(n, bl);
  for (auto &old_pool : old_pools) {
    n = old_pool;
    ::encode(n, bl);
  }
  ::encode(new_up_client, bl, 0);
  ::encode(new_state, bl);
  ::encode(new_weight, bl);
  // for ::encode(new_pg_temp, bl);
  n = new_pg_temp.size();
  ::encode(n, bl);

  for (const auto &pg_temp : new_pg_temp) {
    old_pg_t opg = pg_temp.first.get_old_pg();
    ::encode(opg, bl);
    ::encode(pg_temp.second, bl);
  }
}

void OSDMap::Incremental::encode_classic(bufferlist& bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_PGID64) == 0) {
    encode_client_old(bl);
    return;
  }

  // base
  __u16 v = 6;
  ::encode(v, bl);
  ::encode(fsid, bl);
  ::encode(epoch, bl);
  ::encode(modified, bl);
  ::encode(new_pool_max, bl);
  ::encode(new_flags, bl);
  ::encode(fullmap, bl);
  ::encode(crush, bl);

  ::encode(new_max_osd, bl);
  ::encode(new_pools, bl, features);
  ::encode(new_pool_names, bl);
  ::encode(old_pools, bl);
  ::encode(new_up_client, bl, features);
  ::encode(new_state, bl);
  ::encode(new_weight, bl);
  ::encode(new_pg_temp, bl);

  // extended
  __u16 ev = 10;
  ::encode(ev, bl);
  ::encode(new_hb_back_up, bl, features);
  ::encode(new_up_thru, bl);
  ::encode(new_last_clean_interval, bl);
  ::encode(new_lost, bl);
  ::encode(new_blacklist, bl, features);
  ::encode(old_blacklist, bl, features);
  ::encode(new_up_cluster, bl, features);
  ::encode(cluster_snapshot, bl);
  ::encode(new_uuid, bl);
  ::encode(new_xinfo, bl);
  ::encode(new_hb_front_up, bl, features);
}

void OSDMap::Incremental::encode(bufferlist& bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_OSDMAP_ENC) == 0) {
    encode_classic(bl, features);
    return;
  }

  // only a select set of callers should *ever* be encoding new
  // OSDMaps.  others should be passing around the canonical encoded
  // buffers from on high.  select out those callers by passing in an
  // "impossible" feature bit.
  assert(features & CEPH_FEATURE_RESERVED);
  features &= ~CEPH_FEATURE_RESERVED;

  size_t start_offset = bl.length();
  size_t tail_offset;
  buffer::list::iterator crc_it;

  // meta-encoding: how we include client-used and osd-specific data
  ENCODE_START(8, 7, bl);

  {
    uint8_t v = 4;
    if (!HAVE_FEATURE(features, SERVER_LUMINOUS)) {
      v = 3;
    }
    ENCODE_START(v, 1, bl); // client-usable data
    ::encode(fsid, bl);
    ::encode(epoch, bl);
    ::encode(modified, bl);
    ::encode(new_pool_max, bl);
    ::encode(new_flags, bl);
    ::encode(fullmap, bl);
    ::encode(crush, bl);

    ::encode(new_max_osd, bl);
    ::encode(new_pools, bl, features);
    ::encode(new_pool_names, bl);
    ::encode(old_pools, bl);
    ::encode(new_up_client, bl, features);
    ::encode(new_state, bl);
    ::encode(new_weight, bl);
    ::encode(new_pg_temp, bl);
    ::encode(new_primary_temp, bl);
    ::encode(new_primary_affinity, bl);
    ::encode(new_erasure_code_profiles, bl);
    ::encode(old_erasure_code_profiles, bl);
    if (v >= 4) {
      ::encode(new_pg_upmap, bl);
      ::encode(old_pg_upmap, bl);
      ::encode(new_pg_upmap_items, bl);
      ::encode(old_pg_upmap_items, bl);
    }
    ENCODE_FINISH(bl); // client-usable data
  }

  {
    uint8_t target_v = 4;
    if (!HAVE_FEATURE(features, SERVER_LUMINOUS)) {
      target_v = 2;
    }
    ENCODE_START(target_v, 1, bl); // extended, osd-only data
    ::encode(new_hb_back_up, bl, features);
    ::encode(new_up_thru, bl);
    ::encode(new_last_clean_interval, bl);
    ::encode(new_lost, bl);
    ::encode(new_blacklist, bl, features);
    ::encode(old_blacklist, bl, features);
    ::encode(new_up_cluster, bl, features);
    ::encode(cluster_snapshot, bl);
    ::encode(new_uuid, bl);
    ::encode(new_xinfo, bl);
    ::encode(new_hb_front_up, bl, features);
    ::encode(features, bl);         // NOTE: features arg, not the member
    if (target_v >= 3) {
      ::encode(new_nearfull_ratio, bl);
      ::encode(new_full_ratio, bl);
      ::encode(new_backfillfull_ratio, bl);
    }
    ENCODE_FINISH(bl); // osd-only data
  }

  ::encode((uint32_t)0, bl); // dummy inc_crc
  crc_it = bl.end();
  crc_it.advance(-4);
  tail_offset = bl.length();

  ::encode(full_crc, bl);

  ENCODE_FINISH(bl); // meta-encoding wrapper

  // fill in crc
  bufferlist front;
  front.substr_of(bl, start_offset, crc_it.get_off() - start_offset);
  inc_crc = front.crc32c(-1);
  bufferlist tail;
  tail.substr_of(bl, tail_offset, bl.length() - tail_offset);
  inc_crc = tail.crc32c(inc_crc);
  ceph_le32 crc_le;
  crc_le = inc_crc;
  crc_it.copy_in(4, (char*)&crc_le);
  have_crc = true;
}

void OSDMap::Incremental::decode_classic(bufferlist::iterator &p)
{
  __u32 n, t;
  // base
  __u16 v;
  ::decode(v, p);
  ::decode(fsid, p);
  ::decode(epoch, p);
  ::decode(modified, p);
  if (v == 4 || v == 5) {
    ::decode(n, p);
    new_pool_max = n;
  } else if (v >= 6)
    ::decode(new_pool_max, p);
  ::decode(new_flags, p);
  ::decode(fullmap, p);
  ::decode(crush, p);

  ::decode(new_max_osd, p);
  if (v < 6) {
    new_pools.clear();
    ::decode(n, p);
    while (n--) {
      ::decode(t, p);
      ::decode(new_pools[t], p);
    }
  } else {
    ::decode(new_pools, p);
  }
  if (v == 5) {
    new_pool_names.clear();
    ::decode(n, p);
    while (n--) {
      ::decode(t, p);
      ::decode(new_pool_names[t], p);
    }
  } else if (v >= 6) {
    ::decode(new_pool_names, p);
  }
  if (v < 6) {
    old_pools.clear();
    ::decode(n, p);
    while (n--) {
      ::decode(t, p);
      old_pools.insert(t);
    }
  } else {
    ::decode(old_pools, p);
  }
  ::decode(new_up_client, p);
  ::decode(new_state, p);
  ::decode(new_weight, p);

  if (v < 6) {
    new_pg_temp.clear();
    ::decode(n, p);
    while (n--) {
      old_pg_t opg;
      ::decode_raw(opg, p);
      ::decode(new_pg_temp[pg_t(opg)], p);
    }
  } else {
    ::decode(new_pg_temp, p);
  }

  // decode short map, too.
  if (v == 5 && p.end())
    return;

  // extended
  __u16 ev = 0;
  if (v >= 5)
    ::decode(ev, p);
  ::decode(new_hb_back_up, p);
  if (v < 5)
    ::decode(new_pool_names, p);
  ::decode(new_up_thru, p);
  ::decode(new_last_clean_interval, p);
  ::decode(new_lost, p);
  ::decode(new_blacklist, p);
  ::decode(old_blacklist, p);
  if (ev >= 6)
    ::decode(new_up_cluster, p);
  if (ev >= 7)
    ::decode(cluster_snapshot, p);
  if (ev >= 8)
    ::decode(new_uuid, p);
  if (ev >= 9)
    ::decode(new_xinfo, p);
  if (ev >= 10)
    ::decode(new_hb_front_up, p);
}

void OSDMap::Incremental::decode(bufferlist::iterator& bl)
{
  /**
   * Older encodings of the Incremental had a single struct_v which
   * covered the whole encoding, and was prior to our modern
   * stuff which includes a compatv and a size. So if we see
   * a struct_v < 7, we must rewind to the beginning and use our
   * classic decoder.
   */
  size_t start_offset = bl.get_off();
  size_t tail_offset = 0;
  bufferlist crc_front, crc_tail;

  DECODE_START_LEGACY_COMPAT_LEN(8, 7, 7, bl); // wrapper
  if (struct_v < 7) {
    int struct_v_size = sizeof(struct_v);
    bl.advance(-struct_v_size);
    decode_classic(bl);
    encode_features = 0;
    if (struct_v >= 6)
      encode_features = CEPH_FEATURE_PGID64;
    else
      encode_features = 0;
    return;
  }
  {
    DECODE_START(4, bl); // client-usable data
    ::decode(fsid, bl);
    ::decode(epoch, bl);
    ::decode(modified, bl);
    ::decode(new_pool_max, bl);
    ::decode(new_flags, bl);
    ::decode(fullmap, bl);
    ::decode(crush, bl);

    ::decode(new_max_osd, bl);
    ::decode(new_pools, bl);
    ::decode(new_pool_names, bl);
    ::decode(old_pools, bl);
    ::decode(new_up_client, bl);
    ::decode(new_state, bl);
    ::decode(new_weight, bl);
    ::decode(new_pg_temp, bl);
    ::decode(new_primary_temp, bl);
    if (struct_v >= 2)
      ::decode(new_primary_affinity, bl);
    else
      new_primary_affinity.clear();
    if (struct_v >= 3) {
      ::decode(new_erasure_code_profiles, bl);
      ::decode(old_erasure_code_profiles, bl);
    } else {
      new_erasure_code_profiles.clear();
      old_erasure_code_profiles.clear();
    }
    if (struct_v >= 4) {
      ::decode(new_pg_upmap, bl);
      ::decode(old_pg_upmap, bl);
      ::decode(new_pg_upmap_items, bl);
      ::decode(old_pg_upmap_items, bl);
    }
    DECODE_FINISH(bl); // client-usable data
  }

  {
    DECODE_START(4, bl); // extended, osd-only data
    ::decode(new_hb_back_up, bl);
    ::decode(new_up_thru, bl);
    ::decode(new_last_clean_interval, bl);
    ::decode(new_lost, bl);
    ::decode(new_blacklist, bl);
    ::decode(old_blacklist, bl);
    ::decode(new_up_cluster, bl);
    ::decode(cluster_snapshot, bl);
    ::decode(new_uuid, bl);
    ::decode(new_xinfo, bl);
    ::decode(new_hb_front_up, bl);
    if (struct_v >= 2)
      ::decode(encode_features, bl);
    else
      encode_features = CEPH_FEATURE_PGID64 | CEPH_FEATURE_OSDMAP_ENC;
    if (struct_v >= 3) {
      ::decode(new_nearfull_ratio, bl);
      ::decode(new_full_ratio, bl);
    } else {
      new_nearfull_ratio = -1;
      new_full_ratio = -1;
    }
    if (struct_v >= 4) {
      ::decode(new_backfillfull_ratio, bl);
    } else {
      new_backfillfull_ratio = -1;
    }
    DECODE_FINISH(bl); // osd-only data
  }

  if (struct_v >= 8) {
    have_crc = true;
    crc_front.substr_of(bl.get_bl(), start_offset, bl.get_off() - start_offset);
    ::decode(inc_crc, bl);
    tail_offset = bl.get_off();
    ::decode(full_crc, bl);
  } else {
    have_crc = false;
    full_crc = 0;
    inc_crc = 0;
  }

  DECODE_FINISH(bl); // wrapper

  if (have_crc) {
    // verify crc
    uint32_t actual = crc_front.crc32c(-1);
    if (tail_offset < bl.get_off()) {
      bufferlist tail;
      tail.substr_of(bl.get_bl(), tail_offset, bl.get_off() - tail_offset);
      actual = tail.crc32c(actual);
    }
    if (inc_crc != actual) {
      ostringstream ss;
      ss << "bad crc, actual " << actual << " != expected " << inc_crc;
      string s = ss.str();
      throw buffer::malformed_input(s.c_str());
    }
  }
}

void OSDMap::Incremental::dump(Formatter *f) const
{
  f->dump_int("epoch", epoch);
  f->dump_stream("fsid") << fsid;
  f->dump_stream("modified") << modified;
  f->dump_int("new_pool_max", new_pool_max);
  f->dump_int("new_flags", new_flags);
  f->dump_float("new_full_ratio", new_full_ratio);
  f->dump_float("new_nearfull_ratio", new_nearfull_ratio);
  f->dump_float("new_backfillfull_ratio", new_backfillfull_ratio);

  if (fullmap.length()) {
    f->open_object_section("full_map");
    OSDMap full;
    bufferlist fbl = fullmap;  // kludge around constness.
    auto p = fbl.begin();
    full.decode(p);
    full.dump(f);
    f->close_section();
  }
  if (crush.length()) {
    f->open_object_section("crush");
    CrushWrapper c;
    bufferlist tbl = crush;  // kludge around constness.
    auto p = tbl.begin();
    c.decode(p);
    c.dump(f);
    f->close_section();
  }

  f->dump_int("new_max_osd", new_max_osd);

  f->open_array_section("new_pools");

  for (const auto &new_pool : new_pools) {
    f->open_object_section("pool");
    f->dump_int("pool", new_pool.first);
    new_pool.second.dump(f);
    f->close_section();
  }
  f->close_section();
  f->open_array_section("new_pool_names");

  for (const auto &new_pool_name : new_pool_names) {
    f->open_object_section("pool_name");
    f->dump_int("pool", new_pool_name.first);
    f->dump_string("name", new_pool_name.second);
    f->close_section();
  }
  f->close_section();
  f->open_array_section("old_pools");

  for (const auto &old_pool : old_pools)
    f->dump_int("pool", old_pool);
  f->close_section();

  f->open_array_section("new_up_osds");

  for (const auto &upclient : new_up_client) {
    f->open_object_section("osd");
    f->dump_int("osd", upclient.first);
    f->dump_stream("public_addr") << upclient.second;
    f->dump_stream("cluster_addr") << new_up_cluster.find(upclient.first)->second;
    f->dump_stream("heartbeat_back_addr") << new_hb_back_up.find(upclient.first)->second;
    map<int32_t, entity_addr_t>::const_iterator q;
    if ((q = new_hb_front_up.find(upclient.first)) != new_hb_front_up.end())
      f->dump_stream("heartbeat_front_addr") << q->second;
    f->close_section();
  }
  f->close_section();

  f->open_array_section("new_weight");

  for (const auto &weight : new_weight) {
    f->open_object_section("osd");
    f->dump_int("osd", weight.first);
    f->dump_int("weight", weight.second);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("osd_state_xor");
  for (const auto &ns : new_state) {
    f->open_object_section("osd");
    f->dump_int("osd", ns.first);
    set<string> st;
    calc_state_set(new_state.find(ns.first)->second, st);
    f->open_array_section("state_xor");
    for (auto &state : st)
      f->dump_string("state", state);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("new_pg_temp");

  for (const auto &pg_temp : new_pg_temp) {
    f->open_object_section("pg");
    f->dump_stream("pgid") << pg_temp.first;
    f->open_array_section("osds");

    for (const auto &osd : pg_temp.second)
      f->dump_int("osd", osd);
    f->close_section();
    f->close_section();    
  }
  f->close_section();

  f->open_array_section("primary_temp");

  for (const auto &primary_temp : new_primary_temp) {
    f->dump_stream("pgid") << primary_temp.first;
    f->dump_int("osd", primary_temp.second);
  }
  f->close_section(); // primary_temp

  f->open_array_section("new_pg_upmap");
  for (auto& i : new_pg_upmap) {
    f->open_object_section("mapping");
    f->dump_stream("pgid") << i.first;
    f->open_array_section("osds");
    for (auto osd : i.second) {
      f->dump_int("osd", osd);
    }
    f->close_section();
    f->close_section();
  }
  f->close_section();
  f->open_array_section("old_pg_upmap");
  for (auto& i : old_pg_upmap) {
    f->dump_stream("pgid") << i;
  }
  f->close_section();

  f->open_array_section("new_pg_upmap_items");
  for (auto& i : new_pg_upmap_items) {
    f->open_object_section("mapping");
    f->dump_stream("pgid") << i.first;
    f->open_array_section("mappings");
    for (auto& p : i.second) {
      f->open_object_section("mapping");
      f->dump_int("from", p.first);
      f->dump_int("to", p.second);
      f->close_section();
    }
    f->close_section();
    f->close_section();
  }
  f->close_section();
  f->open_array_section("old_pg_upmap_items");
  for (auto& i : old_pg_upmap_items) {
    f->dump_stream("pgid") << i;
  }
  f->close_section();

  f->open_array_section("new_up_thru");

  for (const auto &up_thru : new_up_thru) {
    f->open_object_section("osd");
    f->dump_int("osd", up_thru.first);
    f->dump_int("up_thru", up_thru.second);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("new_lost");

  for (const auto &lost : new_lost) {
    f->open_object_section("osd");
    f->dump_int("osd", lost.first);
    f->dump_int("epoch_lost", lost.second);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("new_last_clean_interval");

  for (const auto &last_clean_interval : new_last_clean_interval) {
    f->open_object_section("osd");
    f->dump_int("osd", last_clean_interval.first);
    f->dump_int("first", last_clean_interval.second.first);
    f->dump_int("last", last_clean_interval.second.second);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("new_blacklist");
  for (const auto &blist : new_blacklist) {
    stringstream ss;
    ss << blist.first;
    f->dump_stream(ss.str().c_str()) << blist.second;
  }
  f->close_section();
  f->open_array_section("old_blacklist");
  for (const auto &blist : old_blacklist)
    f->dump_stream("addr") << blist;
  f->close_section();

  f->open_array_section("new_xinfo");
  for (const auto &xinfo : new_xinfo) {
    f->open_object_section("xinfo");
    f->dump_int("osd", xinfo.first);
    xinfo.second.dump(f);
    f->close_section();
  }
  f->close_section();

  if (cluster_snapshot.size())
    f->dump_string("cluster_snapshot", cluster_snapshot);

  f->open_array_section("new_uuid");
  for (const auto &uuid : new_uuid) {
    f->open_object_section("osd");
    f->dump_int("osd", uuid.first);
    f->dump_stream("uuid") << uuid.second;
    f->close_section();
  }
  f->close_section();

  OSDMap::dump_erasure_code_profiles(new_erasure_code_profiles, f);
  f->open_array_section("old_erasure_code_profiles");
  for (const auto &erasure_code_profile : old_erasure_code_profiles) {
    f->dump_string("old", erasure_code_profile.c_str());
  }
  f->close_section();
}

void OSDMap::Incremental::generate_test_instances(list<Incremental*>& o)
{
  o.push_back(new Incremental);
}

// ----------------------------------
// OSDMap

void OSDMap::set_epoch(epoch_t e)
{
  epoch = e;
  for (auto &pool : pools)
    pool.second.last_change = e;
}

bool OSDMap::is_blacklisted(const entity_addr_t& a) const
{
  if (blacklist.empty())
    return false;

  // this specific instance?
  if (blacklist.count(a))
    return true;

  // is entire ip blacklisted?
  if (a.is_ip()) {
    entity_addr_t b = a;
    b.set_port(0);
    b.set_nonce(0);
    if (blacklist.count(b)) {
      return true;
    }
  }

  return false;
}

void OSDMap::get_blacklist(list<pair<entity_addr_t,utime_t> > *bl) const
{
   std::copy(blacklist.begin(), blacklist.end(), std::back_inserter(*bl));
}

void OSDMap::set_max_osd(int m)
{
  int o = max_osd;
  max_osd = m;
  osd_state.resize(m);
  osd_weight.resize(m);
  for (; o<max_osd; o++) {
    osd_state[o] = 0;
    osd_weight[o] = CEPH_OSD_OUT;
  }
  osd_info.resize(m);
  osd_xinfo.resize(m);
  osd_addrs->client_addr.resize(m);
  osd_addrs->cluster_addr.resize(m);
  osd_addrs->hb_back_addr.resize(m);
  osd_addrs->hb_front_addr.resize(m);
  osd_uuid->resize(m);
  if (osd_primary_affinity)
    osd_primary_affinity->resize(m, CEPH_OSD_DEFAULT_PRIMARY_AFFINITY);

  calc_num_osds();
}

int OSDMap::calc_num_osds()
{
  num_osd = 0;
  num_up_osd = 0;
  num_in_osd = 0;
  for (int i=0; i<max_osd; i++) {
    if (osd_state[i] & CEPH_OSD_EXISTS) {
      ++num_osd;
      if (osd_state[i] & CEPH_OSD_UP) {
	++num_up_osd;
      }
      if (get_weight(i) != CEPH_OSD_OUT) {
	++num_in_osd;
      }
    }
  }
  return num_osd;
}

void OSDMap::count_full_nearfull_osds(int *full, int *backfill, int *nearfull) const
{
  *full = 0;
  *backfill = 0;
  *nearfull = 0;
  for (int i = 0; i < max_osd; ++i) {
    if (exists(i) && is_up(i) && is_in(i)) {
      if (osd_state[i] & CEPH_OSD_FULL)
	++(*full);
      else if (osd_state[i] & CEPH_OSD_BACKFILLFULL)
	++(*backfill);
      else if (osd_state[i] & CEPH_OSD_NEARFULL)
	++(*nearfull);
    }
  }
}

static bool get_osd_utilization(const ceph::unordered_map<int32_t,osd_stat_t> &osd_stat,
   int id, int64_t* kb, int64_t* kb_used, int64_t* kb_avail) {
    auto p = osd_stat.find(id);
    if (p == osd_stat.end())
      return false;
    *kb = p->second.kb;
    *kb_used = p->second.kb_used;
    *kb_avail = p->second.kb_avail;
    return *kb > 0;
}

void OSDMap::get_full_osd_util(const ceph::unordered_map<int32_t,osd_stat_t> &osd_stat,
     map<int, float> *full, map<int, float> *backfill, map<int, float> *nearfull) const
{
  full->clear();
  backfill->clear();
  nearfull->clear();
  for (int i = 0; i < max_osd; ++i) {
    if (exists(i) && is_up(i) && is_in(i)) {
      int64_t kb, kb_used, kb_avail;
      if (osd_state[i] & CEPH_OSD_FULL) {
        if (get_osd_utilization(osd_stat, i, &kb, &kb_used, &kb_avail))
	  full->emplace(i, (float)kb_used / (float)kb);
      } else if (osd_state[i] & CEPH_OSD_BACKFILLFULL) {
        if (get_osd_utilization(osd_stat, i, &kb, &kb_used, &kb_avail))
	  backfill->emplace(i, (float)kb_used / (float)kb);
      } else if (osd_state[i] & CEPH_OSD_NEARFULL) {
        if (get_osd_utilization(osd_stat, i, &kb, &kb_used, &kb_avail))
	  nearfull->emplace(i, (float)kb_used / (float)kb);
      }
    }
  }
}

void OSDMap::get_all_osds(set<int32_t>& ls) const
{
  for (int i=0; i<max_osd; i++)
    if (exists(i))
      ls.insert(i);
}

void OSDMap::get_up_osds(set<int32_t>& ls) const
{
  for (int i = 0; i < max_osd; i++) {
    if (is_up(i))
      ls.insert(i);
  }
}

void OSDMap::calc_state_set(int state, set<string>& st)
{
  unsigned t = state;
  for (unsigned s = 1; t; s <<= 1) {
    if (t & s) {
      t &= ~s;
      st.insert(ceph_osd_state_name(s));
    }
  }
}

void OSDMap::adjust_osd_weights(const map<int,double>& weights, Incremental& inc) const
{
  float max = 0;
  for (const auto &weight : weights) {
    if (weight.second > max)
      max = weight.second;
  }

  for (const auto &weight : weights) {
    inc.new_weight[weight.first] = (unsigned)((weight.second / max) * CEPH_OSD_IN);
  }
}

int OSDMap::identify_osd(const entity_addr_t& addr) const
{
  for (int i=0; i<max_osd; i++)
    if (exists(i) && (get_addr(i) == addr || get_cluster_addr(i) == addr))
      return i;
  return -1;
}

int OSDMap::identify_osd(const uuid_d& u) const
{
  for (int i=0; i<max_osd; i++)
    if (exists(i) && get_uuid(i) == u)
      return i;
  return -1;
}

int OSDMap::identify_osd_on_all_channels(const entity_addr_t& addr) const
{
  for (int i=0; i<max_osd; i++)
    if (exists(i) && (get_addr(i) == addr || get_cluster_addr(i) == addr ||
	get_hb_back_addr(i) == addr || get_hb_front_addr(i) == addr))
      return i;
  return -1;
}

int OSDMap::find_osd_on_ip(const entity_addr_t& ip) const
{
  for (int i=0; i<max_osd; i++)
    if (exists(i) && (get_addr(i).is_same_host(ip) || get_cluster_addr(i).is_same_host(ip)))
      return i;
  return -1;
}


uint64_t OSDMap::get_features(int entity_type, uint64_t *pmask) const
{
  uint64_t features = 0;  // things we actually have
  uint64_t mask = 0;      // things we could have

  if (crush->has_nondefault_tunables())
    features |= CEPH_FEATURE_CRUSH_TUNABLES;
  if (crush->has_nondefault_tunables2())
    features |= CEPH_FEATURE_CRUSH_TUNABLES2;
  if (crush->has_nondefault_tunables3())
    features |= CEPH_FEATURE_CRUSH_TUNABLES3;
  if (crush->has_v4_buckets())
    features |= CEPH_FEATURE_CRUSH_V4;
  if (crush->has_nondefault_tunables5())
    features |= CEPH_FEATURE_CRUSH_TUNABLES5;
  mask |= CEPH_FEATURES_CRUSH;

  if (!pg_upmap.empty() || !pg_upmap_items.empty())
    features |= CEPH_FEATUREMASK_OSDMAP_REMAP;
  mask |= CEPH_FEATUREMASK_OSDMAP_REMAP;

  for (auto &pool: pools) {
    if (pool.second.has_flag(pg_pool_t::FLAG_HASHPSPOOL)) {
      features |= CEPH_FEATURE_OSDHASHPSPOOL;
    }
    if (pool.second.is_erasure() &&
	entity_type != CEPH_ENTITY_TYPE_CLIENT) { // not for clients
      features |= CEPH_FEATURE_OSD_ERASURE_CODES;
    }
    if (!pool.second.tiers.empty() ||
	pool.second.is_tier()) {
      features |= CEPH_FEATURE_OSD_CACHEPOOL;
    }
    int ruleid = crush->find_rule(pool.second.get_crush_ruleset(),
				  pool.second.get_type(),
				  pool.second.get_size());
    if (ruleid >= 0) {
      if (crush->is_v2_rule(ruleid))
	features |= CEPH_FEATURE_CRUSH_V2;
      if (crush->is_v3_rule(ruleid))
	features |= CEPH_FEATURE_CRUSH_TUNABLES3;
      if (crush->is_v5_rule(ruleid))
	features |= CEPH_FEATURE_CRUSH_TUNABLES5;
    }
  }
  if (entity_type == CEPH_ENTITY_TYPE_OSD) {
    for (auto &erasure_code_profile : erasure_code_profiles) {
      const map<string,string> &profile = erasure_code_profile.second;
      const auto &plugin = profile.find("plugin");
      if (plugin != profile.end()) {
	if (plugin->second == "isa" || plugin->second == "lrc")
	  features |= CEPH_FEATURE_ERASURE_CODE_PLUGINS_V2;
	if (plugin->second == "shec")
	  features |= CEPH_FEATURE_ERASURE_CODE_PLUGINS_V3;
      }
    }
  }
  mask |= CEPH_FEATURE_OSDHASHPSPOOL | CEPH_FEATURE_OSD_CACHEPOOL;
  if (entity_type != CEPH_ENTITY_TYPE_CLIENT)
    mask |= CEPH_FEATURE_OSD_ERASURE_CODES;

  if (osd_primary_affinity) {
    for (int i = 0; i < max_osd; ++i) {
      if ((*osd_primary_affinity)[i] != CEPH_OSD_DEFAULT_PRIMARY_AFFINITY) {
	features |= CEPH_FEATURE_OSD_PRIMARY_AFFINITY;
	break;
      }
    }
  }
  mask |= CEPH_FEATURE_OSD_PRIMARY_AFFINITY;

  if (entity_type == CEPH_ENTITY_TYPE_OSD) {
    const uint64_t jewel_features = CEPH_FEATURE_SERVER_JEWEL;
    if (test_flag(CEPH_OSDMAP_REQUIRE_JEWEL)) {
      features |= jewel_features;
    }
    mask |= jewel_features;

    const uint64_t kraken_features = CEPH_FEATUREMASK_SERVER_KRAKEN
      | CEPH_FEATURE_MSG_ADDR2;
    if (test_flag(CEPH_OSDMAP_REQUIRE_KRAKEN)) {
      features |= kraken_features;
    }
    mask |= kraken_features;
  }

  if (pmask)
    *pmask = mask;
  return features;
}

void OSDMap::_calc_up_osd_features()
{
  bool first = true;
  cached_up_osd_features = 0;
  for (int osd = 0; osd < max_osd; ++osd) {
    if (!is_up(osd))
      continue;
    const osd_xinfo_t &xi = get_xinfo(osd);
    if (first) {
      cached_up_osd_features = xi.features;
      first = false;
    } else {
      cached_up_osd_features &= xi.features;
    }
  }
}

uint64_t OSDMap::get_up_osd_features() const
{
  return cached_up_osd_features;
}

void OSDMap::dedup(const OSDMap *o, OSDMap *n)
{
  if (o->epoch == n->epoch)
    return;

  int diff = 0;

  // do addrs match?
  if (o->max_osd != n->max_osd)
    diff++;
  for (int i = 0; i < o->max_osd && i < n->max_osd; i++) {
    if ( n->osd_addrs->client_addr[i] &&  o->osd_addrs->client_addr[i] &&
	*n->osd_addrs->client_addr[i] == *o->osd_addrs->client_addr[i])
      n->osd_addrs->client_addr[i] = o->osd_addrs->client_addr[i];
    else
      diff++;
    if ( n->osd_addrs->cluster_addr[i] &&  o->osd_addrs->cluster_addr[i] &&
	*n->osd_addrs->cluster_addr[i] == *o->osd_addrs->cluster_addr[i])
      n->osd_addrs->cluster_addr[i] = o->osd_addrs->cluster_addr[i];
    else
      diff++;
    if ( n->osd_addrs->hb_back_addr[i] &&  o->osd_addrs->hb_back_addr[i] &&
	*n->osd_addrs->hb_back_addr[i] == *o->osd_addrs->hb_back_addr[i])
      n->osd_addrs->hb_back_addr[i] = o->osd_addrs->hb_back_addr[i];
    else
      diff++;
    if ( n->osd_addrs->hb_front_addr[i] &&  o->osd_addrs->hb_front_addr[i] &&
	*n->osd_addrs->hb_front_addr[i] == *o->osd_addrs->hb_front_addr[i])
      n->osd_addrs->hb_front_addr[i] = o->osd_addrs->hb_front_addr[i];
    else
      diff++;
  }
  if (diff == 0) {
    // zoinks, no differences at all!
    n->osd_addrs = o->osd_addrs;
  }

  // does crush match?
  bufferlist oc, nc;
  ::encode(*o->crush, oc, CEPH_FEATURES_SUPPORTED_DEFAULT);
  ::encode(*n->crush, nc, CEPH_FEATURES_SUPPORTED_DEFAULT);
  if (oc.contents_equal(nc)) {
    n->crush = o->crush;
  }

  // does pg_temp match?
  if (o->pg_temp->size() == n->pg_temp->size()) {
    if (*o->pg_temp == *n->pg_temp)
      n->pg_temp = o->pg_temp;
  }

  // does primary_temp match?
  if (o->primary_temp->size() == n->primary_temp->size()) {
    if (*o->primary_temp == *n->primary_temp)
      n->primary_temp = o->primary_temp;
  }

  // do uuids match?
  if (o->osd_uuid->size() == n->osd_uuid->size() &&
      *o->osd_uuid == *n->osd_uuid)
    n->osd_uuid = o->osd_uuid;
}

void OSDMap::clean_temps(CephContext *cct,
			 const OSDMap& osdmap, Incremental *pending_inc)
{
  ldout(cct, 10) << __func__ << dendl;
  OSDMap tmpmap;
  tmpmap.deepish_copy_from(osdmap);
  tmpmap.apply_incremental(*pending_inc);

  for (auto pg : *tmpmap.pg_temp) {
    // if pool does not exist, remove any existing pg_temps associated with
    // it.  we don't care about pg_temps on the pending_inc either; if there
    // are new_pg_temp entries on the pending, clear them out just as well.
    if (!osdmap.have_pg_pool(pg.first.pool())) {
      ldout(cct, 10) << __func__ << " removing pg_temp " << pg.first
		     << " for nonexistent pool " << pg.first.pool() << dendl;
      pending_inc->new_pg_temp[pg.first].clear();
      continue;
    }
    // all osds down?
    unsigned num_up = 0;
    for (auto o : pg.second) {
      if (!tmpmap.is_down(o)) {
	++num_up;
	break;
      }
    }
    if (num_up == 0) {
      ldout(cct, 10) << __func__ << "  removing pg_temp " << pg.first
		     << " with all down osds" << pg.second << dendl;
      pending_inc->new_pg_temp[pg.first].clear();
      continue;
    }
    // redundant pg_temp?
    vector<int> raw_up;
    int primary;
    tmpmap.pg_to_raw_up(pg.first, &raw_up, &primary);
    if (raw_up == pg.second) {
      ldout(cct, 10) << __func__ << "  removing pg_temp " << pg.first << " "
		     << pg.second << " that matches raw_up mapping" << dendl;
      if (osdmap.pg_temp->count(pg.first))
	pending_inc->new_pg_temp[pg.first].clear();
      else
	pending_inc->new_pg_temp.erase(pg.first);
    }
  }
  
  for (auto &pg : *tmpmap.primary_temp) {
    // primary down?
    if (tmpmap.is_down(pg.second)) {
      ldout(cct, 10) << __func__ << "  removing primary_temp " << pg.first
		     << " to down " << pg.second << dendl;
      pending_inc->new_primary_temp[pg.first] = -1;
      continue;
    }
    // redundant primary_temp?
    vector<int> real_up, templess_up;
    int real_primary, templess_primary;
    pg_t pgid = pg.first;
    tmpmap.pg_to_acting_osds(pgid, &real_up, &real_primary);
    tmpmap.pg_to_raw_up(pgid, &templess_up, &templess_primary);
    if (real_primary == templess_primary){
      ldout(cct, 10) << __func__ << "  removing primary_temp "
		     << pgid << " -> " << real_primary
		     << " (unnecessary/redundant)" << dendl;
      if (osdmap.primary_temp->count(pgid))
	pending_inc->new_primary_temp[pgid] = -1;
      else
	pending_inc->new_primary_temp.erase(pgid);
    }
  }
}

int OSDMap::apply_incremental(const Incremental &inc)
{
  new_blacklist_entries = false;
  if (inc.epoch == 1)
    fsid = inc.fsid;
  else if (inc.fsid != fsid)
    return -EINVAL;
  
  assert(inc.epoch == epoch+1);

  epoch++;
  modified = inc.modified;

  // full map?
  if (inc.fullmap.length()) {
    bufferlist bl(inc.fullmap);
    decode(bl);
    return 0;
  }

  // nope, incremental.
  if (inc.new_flags >= 0)
    flags = inc.new_flags;

  if (inc.new_max_osd >= 0)
    set_max_osd(inc.new_max_osd);

  if (inc.new_pool_max != -1)
    pool_max = inc.new_pool_max;

  for (const auto &pool : inc.new_pools) {
    pools[pool.first] = pool.second;
    pools[pool.first].last_change = epoch;
  }

  for (const auto &pname : inc.new_pool_names) {
    auto pool_name_entry = pool_name.find(pname.first);
    if (pool_name_entry != pool_name.end()) {
      name_pool.erase(pool_name_entry->second);
      pool_name_entry->second = pname.second;
    } else {
      pool_name[pname.first] = pname.second;
    }
    name_pool[pname.second] = pname.first;
  }
  
  for (const auto &pool : inc.old_pools) {
    pools.erase(pool);
    name_pool.erase(pool_name[pool]);
    pool_name.erase(pool);
  }

  for (const auto &weight : inc.new_weight) {
    set_weight(weight.first, weight.second);

    // if we are marking in, clear the AUTOOUT and NEW bits, and clear
    // xinfo old_weight.
    if (weight.second) {
      osd_state[weight.first] &= ~(CEPH_OSD_AUTOOUT | CEPH_OSD_NEW);
      osd_xinfo[weight.first].old_weight = 0;
    }
  }

  for (const auto &primary_affinity : inc.new_primary_affinity) {
    set_primary_affinity(primary_affinity.first, primary_affinity.second);
  }

  // erasure_code_profiles
  for (const auto &profile : inc.old_erasure_code_profiles)
    erasure_code_profiles.erase(profile);
  
  for (const auto &profile : inc.new_erasure_code_profiles) {
    set_erasure_code_profile(profile.first, profile.second);
  }
  
  // up/down
  for (const auto &state : inc.new_state) {
    const auto osd = state.first;
    int s = state.second ? state.second : CEPH_OSD_UP;
    if ((osd_state[osd] & CEPH_OSD_UP) &&
	(s & CEPH_OSD_UP)) {
      osd_info[osd].down_at = epoch;
      osd_xinfo[osd].down_stamp = modified;
    }
    if ((osd_state[osd] & CEPH_OSD_EXISTS) &&
	(s & CEPH_OSD_EXISTS)) {
      // osd is destroyed; clear out anything interesting.
      (*osd_uuid)[osd] = uuid_d();
      osd_info[osd] = osd_info_t();
      osd_xinfo[osd] = osd_xinfo_t();
      set_primary_affinity(osd, CEPH_OSD_DEFAULT_PRIMARY_AFFINITY);
      osd_addrs->client_addr[osd].reset(new entity_addr_t());
      osd_addrs->cluster_addr[osd].reset(new entity_addr_t());
      osd_addrs->hb_front_addr[osd].reset(new entity_addr_t());
      osd_addrs->hb_back_addr[osd].reset(new entity_addr_t());
      osd_state[osd] = 0;
    } else {
      osd_state[osd] ^= s;
    }
  }

  for (const auto &client : inc.new_up_client) {
    osd_state[client.first] |= CEPH_OSD_EXISTS | CEPH_OSD_UP;
    osd_addrs->client_addr[client.first].reset(new entity_addr_t(client.second));
    if (inc.new_hb_back_up.empty())
      osd_addrs->hb_back_addr[client.first].reset(new entity_addr_t(client.second)); //this is a backward-compatibility hack
    else
      osd_addrs->hb_back_addr[client.first].reset(
	new entity_addr_t(inc.new_hb_back_up.find(client.first)->second));
    const auto j = inc.new_hb_front_up.find(client.first);
    if (j != inc.new_hb_front_up.end())
      osd_addrs->hb_front_addr[client.first].reset(new entity_addr_t(j->second));
    else
      osd_addrs->hb_front_addr[client.first].reset();

    osd_info[client.first].up_from = epoch;
  }

  for (const auto &cluster : inc.new_up_cluster)
    osd_addrs->cluster_addr[cluster.first].reset(new entity_addr_t(cluster.second));

  // info
  for (const auto &thru : inc.new_up_thru)
    osd_info[thru.first].up_thru = thru.second;
  
  for (const auto &interval : inc.new_last_clean_interval) {
    osd_info[interval.first].last_clean_begin = interval.second.first;
    osd_info[interval.first].last_clean_end = interval.second.second;
  }
  
  for (const auto &lost : inc.new_lost)
    osd_info[lost.first].lost_at = lost.second;

  // xinfo
  for (const auto &xinfo : inc.new_xinfo)
    osd_xinfo[xinfo.first] = xinfo.second;

  // uuid
  for (const auto &uuid : inc.new_uuid)
    (*osd_uuid)[uuid.first] = uuid.second;

  // pg rebuild
  for (const auto &pg : inc.new_pg_temp) {
    if (pg.second.empty())
      pg_temp->erase(pg.first);
    else
      (*pg_temp)[pg.first] = pg.second;
  }

  for (const auto &pg : inc.new_primary_temp) {
    if (pg.second == -1)
      primary_temp->erase(pg.first);
    else
      (*primary_temp)[pg.first] = pg.second;
  }

  for (auto& p : inc.new_pg_upmap) {
    pg_upmap[p.first] = p.second;
  }
  for (auto& pg : inc.old_pg_upmap) {
    pg_upmap.erase(pg);
  }
  for (auto& p : inc.new_pg_upmap_items) {
    pg_upmap_items[p.first] = p.second;
  }
  for (auto& pg : inc.old_pg_upmap_items) {
    pg_upmap_items.erase(pg);
  }

  // blacklist
  if (!inc.new_blacklist.empty()) {
    blacklist.insert(inc.new_blacklist.begin(),inc.new_blacklist.end());
    new_blacklist_entries = true;
  }
  for (const auto &addr : inc.old_blacklist)
    blacklist.erase(addr);

  // cluster snapshot?
  if (inc.cluster_snapshot.length()) {
    cluster_snapshot = inc.cluster_snapshot;
    cluster_snapshot_epoch = inc.epoch;
  } else {
    cluster_snapshot.clear();
    cluster_snapshot_epoch = 0;
  }

  if (inc.new_nearfull_ratio >= 0) {
    nearfull_ratio = inc.new_nearfull_ratio;
  }
  if (inc.new_backfillfull_ratio >= 0) {
    backfillfull_ratio = inc.new_backfillfull_ratio;
  }
  if (inc.new_full_ratio >= 0) {
    full_ratio = inc.new_full_ratio;
  }

  // do new crush map last (after up/down stuff)
  if (inc.crush.length()) {
    bufferlist bl(inc.crush);
    auto blp = bl.begin();
    crush.reset(new CrushWrapper);
    crush->decode(blp);
  }

  calc_num_osds();
  _calc_up_osd_features();
  return 0;
}

// mapping
int OSDMap::map_to_pg(
  int64_t poolid,
  const string& name,
  const string& key,
  const string& nspace,
  pg_t *pg) const
{
  // calculate ps (placement seed)
  const pg_pool_t *pool = get_pg_pool(poolid);
  if (!pool)
    return -ENOENT;
  ps_t ps;
  if (!key.empty())
    ps = pool->hash_key(key, nspace);
  else
    ps = pool->hash_key(name, nspace);
  *pg = pg_t(ps, poolid);
  return 0;
}

int OSDMap::object_locator_to_pg(
  const object_t& oid, const object_locator_t& loc, pg_t &pg) const
{
  if (loc.hash >= 0) {
    if (!get_pg_pool(loc.get_pool())) {
      return -ENOENT;
    }
    pg = pg_t(loc.hash, loc.get_pool());
    return 0;
  }
  return map_to_pg(loc.get_pool(), oid.name, loc.key, loc.nspace, &pg);
}

ceph_object_layout OSDMap::make_object_layout(
  object_t oid, int pg_pool, string nspace) const
{
  object_locator_t loc(pg_pool, nspace);

  ceph_object_layout ol;
  pg_t pgid = object_locator_to_pg(oid, loc);
  ol.ol_pgid = pgid.get_old_pg().v;
  ol.ol_stripe_unit = 0;
  return ol;
}

void OSDMap::_remove_nonexistent_osds(const pg_pool_t& pool,
				      vector<int>& osds) const
{
  if (pool.can_shift_osds()) {
    unsigned removed = 0;
    for (unsigned i = 0; i < osds.size(); i++) {
      if (!exists(osds[i])) {
	removed++;
	continue;
      }
      if (removed) {
	osds[i - removed] = osds[i];
      }
    }
    if (removed)
      osds.resize(osds.size() - removed);
  } else {
    for (auto osd : osds) {
      if (!exists(osd))
	osd = CRUSH_ITEM_NONE;
    }
  }
}

int OSDMap::_pg_to_raw_osds(
  const pg_pool_t& pool, pg_t pg,
  vector<int> *osds,
  ps_t *ppps) const
{
  // map to osds[]
  ps_t pps = pool.raw_pg_to_pps(pg);  // placement ps
  unsigned size = pool.get_size();

  // what crush rule?
  int ruleno = crush->find_rule(pool.get_crush_ruleset(), pool.get_type(), size);
  if (ruleno >= 0)
    crush->do_rule(ruleno, pps, *osds, size, osd_weight, pg.pool());

  _remove_nonexistent_osds(pool, *osds);

  if (ppps)
    *ppps = pps;

  return osds->size();
}

int OSDMap::_pick_primary(const vector<int>& osds) const
{
  for (auto osd : osds) {
    if (osd != CRUSH_ITEM_NONE) {
      return osd;
    }
  }
  return -1;
}

void OSDMap::_apply_remap(const pg_pool_t& pi, pg_t raw_pg, vector<int> *raw) const
{
  pg_t pg = pi.raw_pg_to_pg(raw_pg);
  auto p = pg_upmap.find(pg);
  if (p != pg_upmap.end()) {
    // make sure targets aren't marked out
    for (auto osd : p->second) {
      if (osd != CRUSH_ITEM_NONE && osd < max_osd && osd_weight[osd] == 0) {
	// reject/ignore the explicit mapping
	return;
      }
    }
    *raw = p->second;
    return;
  }

  auto q = pg_upmap_items.find(pg);
  if (q != pg_upmap_items.end()) {
    // NOTE: this approach does not allow a bidirectional swap,
    // e.g., [[1,2],[2,1]] applied to [0,1,2] -> [0,2,1].
    for (auto& r : q->second) {
      // make sure the replacement value doesn't already appear
      bool exists = false;
      ssize_t pos = -1;
      for (unsigned i = 0; i < raw->size(); ++i) {
	int osd = (*raw)[i];
	if (osd == r.second) {
	  exists = true;
	  break;
	}
	// ignore mapping if target is marked out (or invalid osd id)
	if (osd == r.first &&
	    pos < 0 &&
	    !(r.second != CRUSH_ITEM_NONE && r.second < max_osd &&
	      osd_weight[r.second] == 0)) {
	  pos = i;
	}
      }
      if (!exists && pos >= 0) {
	(*raw)[pos] = r.second;
	return;
      }
    }
  }
}

// pg -> (up osd list)
void OSDMap::_raw_to_up_osds(const pg_pool_t& pool, const vector<int>& raw,
                             vector<int> *up) const
{
  if (pool.can_shift_osds()) {
    // shift left
    up->clear();
    up->reserve(raw.size());
    for (unsigned i=0; i<raw.size(); i++) {
      if (!exists(raw[i]) || is_down(raw[i]))
	continue;
      up->push_back(raw[i]);
    }
  } else {
    // set down/dne devices to NONE
    up->resize(raw.size());
    for (int i = raw.size() - 1; i >= 0; --i) {
      if (!exists(raw[i]) || is_down(raw[i])) {
	(*up)[i] = CRUSH_ITEM_NONE;
      } else {
	(*up)[i] = raw[i];
      }
    }
  }
}

void OSDMap::_apply_primary_affinity(ps_t seed,
				     const pg_pool_t& pool,
				     vector<int> *osds,
				     int *primary) const
{
  // do we have any non-default primary_affinity values for these osds?
  if (!osd_primary_affinity)
    return;

  bool any = false;
  for (const auto osd : *osds) {
    if (osd != CRUSH_ITEM_NONE &&
	(*osd_primary_affinity)[osd] != CEPH_OSD_DEFAULT_PRIMARY_AFFINITY) {
      any = true;
      break;
    }
  }
  if (!any)
    return;

  // pick the primary.  feed both the seed (for the pg) and the osd
  // into the hash/rng so that a proportional fraction of an osd's pgs
  // get rejected as primary.
  int pos = -1;
  for (unsigned i = 0; i < osds->size(); ++i) {
    int o = (*osds)[i];
    if (o == CRUSH_ITEM_NONE)
      continue;
    unsigned a = (*osd_primary_affinity)[o];
    if (a < CEPH_OSD_MAX_PRIMARY_AFFINITY &&
	(crush_hash32_2(CRUSH_HASH_RJENKINS1,
			seed, o) >> 16) >= a) {
      // we chose not to use this primary.  note it anyway as a
      // fallback in case we don't pick anyone else, but keep looking.
      if (pos < 0)
	pos = i;
    } else {
      pos = i;
      break;
    }
  }
  if (pos < 0)
    return;

  *primary = (*osds)[pos];

  if (pool.can_shift_osds() && pos > 0) {
    // move the new primary to the front.
    for (int i = pos; i > 0; --i) {
      (*osds)[i] = (*osds)[i-1];
    }
    (*osds)[0] = *primary;
  }
}

void OSDMap::_get_temp_osds(const pg_pool_t& pool, pg_t pg,
                            vector<int> *temp_pg, int *temp_primary) const
{
  pg = pool.raw_pg_to_pg(pg);
  const auto p = pg_temp->find(pg);
  temp_pg->clear();
  if (p != pg_temp->end()) {
    for (unsigned i=0; i<p->second.size(); i++) {
      if (!exists(p->second[i]) || is_down(p->second[i])) {
	if (pool.can_shift_osds()) {
	  continue;
	} else {
	  temp_pg->push_back(CRUSH_ITEM_NONE);
	}
      } else {
	temp_pg->push_back(p->second[i]);
      }
    }
  }
  const auto &pp = primary_temp->find(pg);
  *temp_primary = -1;
  if (pp != primary_temp->end()) {
    *temp_primary = pp->second;
  } else if (!temp_pg->empty()) { // apply pg_temp's primary
    for (unsigned i = 0; i < temp_pg->size(); ++i) {
      if ((*temp_pg)[i] != CRUSH_ITEM_NONE) {
	*temp_primary = (*temp_pg)[i];
	break;
      }
    }
  }
}

int OSDMap::pg_to_raw_osds(pg_t pg, vector<int> *raw, int *primary) const
{
  *primary = -1;
  raw->clear();
  const pg_pool_t *pool = get_pg_pool(pg.pool());
  if (!pool)
    return 0;
  int r = _pg_to_raw_osds(*pool, pg, raw, NULL);
  if (primary)
    *primary = _pick_primary(*raw);
  return r;
}

void OSDMap::pg_to_raw_up(pg_t pg, vector<int> *up, int *primary) const
{
  const pg_pool_t *pool = get_pg_pool(pg.pool());
  if (!pool) {
    if (primary)
      *primary = -1;
    if (up)
      up->clear();
    return;
  }
  vector<int> raw;
  ps_t pps;
  _pg_to_raw_osds(*pool, pg, &raw, &pps);
  _apply_remap(*pool, pg, &raw);
  _raw_to_up_osds(*pool, raw, up);
  *primary = _pick_primary(raw);
  _apply_primary_affinity(pps, *pool, up, primary);
}
  
void OSDMap::_pg_to_up_acting_osds(
  const pg_t& pg, vector<int> *up, int *up_primary,
  vector<int> *acting, int *acting_primary,
  bool raw_pg_to_pg) const
{
  const pg_pool_t *pool = get_pg_pool(pg.pool());
  if (!pool ||
      (!raw_pg_to_pg && pg.ps() >= pool->get_pg_num())) {
    if (up)
      up->clear();
    if (up_primary)
      *up_primary = -1;
    if (acting)
      acting->clear();
    if (acting_primary)
      *acting_primary = -1;
    return;
  }
  vector<int> raw;
  vector<int> _up;
  vector<int> _acting;
  int _up_primary;
  int _acting_primary;
  ps_t pps;
  _get_temp_osds(*pool, pg, &_acting, &_acting_primary);
  if (_acting.empty() || up || up_primary) {
    _pg_to_raw_osds(*pool, pg, &raw, &pps);
    _apply_remap(*pool, pg, &raw);
    _raw_to_up_osds(*pool, raw, &_up);
    _up_primary = _pick_primary(_up);
    _apply_primary_affinity(pps, *pool, &_up, &_up_primary);
    if (_acting.empty()) {
      _acting = _up;
      if (_acting_primary == -1) {
        _acting_primary = _up_primary;
      }
    }
  
    if (up)
      up->swap(_up);
    if (up_primary)
      *up_primary = _up_primary;
  }

  if (acting)
    acting->swap(_acting);
  if (acting_primary)
    *acting_primary = _acting_primary;
}

int OSDMap::calc_pg_rank(int osd, const vector<int>& acting, int nrep)
{
  if (!nrep)
    nrep = acting.size();
  for (int i=0; i<nrep; i++) 
    if (acting[i] == osd)
      return i;
  return -1;
}

int OSDMap::calc_pg_role(int osd, const vector<int>& acting, int nrep)
{
  return calc_pg_rank(osd, acting, nrep);
}

bool OSDMap::primary_changed(
  int oldprimary,
  const vector<int> &oldacting,
  int newprimary,
  const vector<int> &newacting)
{
  if (oldacting.empty() && newacting.empty())
    return false;    // both still empty
  if (oldacting.empty() ^ newacting.empty())
    return true;     // was empty, now not, or vice versa
  if (oldprimary != newprimary)
    return true;     // primary changed
  if (calc_pg_rank(oldprimary, oldacting) !=
      calc_pg_rank(newprimary, newacting))
    return true;
  return false;      // same primary (tho replicas may have changed)
}


// serialize, unserialize
void OSDMap::encode_client_old(bufferlist& bl) const
{
  __u16 v = 5;
  ::encode(v, bl);

  // base
  ::encode(fsid, bl);
  ::encode(epoch, bl);
  ::encode(created, bl);
  ::encode(modified, bl);

  // for ::encode(pools, bl);
  __u32 n = pools.size();
  ::encode(n, bl);

  for (const auto &pool : pools) {
    n = pool.first;
    ::encode(n, bl);
    ::encode(pool.second, bl, 0);
  }
  // for ::encode(pool_name, bl);
  n = pool_name.size();
  ::encode(n, bl);
  for (const auto &pname : pool_name) {
    n = pname.first;
    ::encode(n, bl);
    ::encode(pname.second, bl);
  }
  // for ::encode(pool_max, bl);
  n = pool_max;
  ::encode(n, bl);

  ::encode(flags, bl);

  ::encode(max_osd, bl);
  ::encode(osd_state, bl);
  ::encode(osd_weight, bl);
  ::encode(osd_addrs->client_addr, bl, 0);

  // for ::encode(pg_temp, bl);
  n = pg_temp->size();
  ::encode(n, bl);
  for (const auto pg : *pg_temp) {
    old_pg_t opg = pg.first.get_old_pg();
    ::encode(opg, bl);
    ::encode(pg.second, bl);
  }

  // crush
  bufferlist cbl;
  crush->encode(cbl, 0 /* legacy (no) features */);
  ::encode(cbl, bl);
}

void OSDMap::encode_classic(bufferlist& bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_PGID64) == 0) {
    encode_client_old(bl);
    return;
  }

  __u16 v = 6;
  ::encode(v, bl);

  // base
  ::encode(fsid, bl);
  ::encode(epoch, bl);
  ::encode(created, bl);
  ::encode(modified, bl);

  ::encode(pools, bl, features);
  ::encode(pool_name, bl);
  ::encode(pool_max, bl);

  ::encode(flags, bl);

  ::encode(max_osd, bl);
  ::encode(osd_state, bl);
  ::encode(osd_weight, bl);
  ::encode(osd_addrs->client_addr, bl, features);

  ::encode(*pg_temp, bl);

  // crush
  bufferlist cbl;
  crush->encode(cbl, 0 /* legacy (no) features */);
  ::encode(cbl, bl);

  // extended
  __u16 ev = 10;
  ::encode(ev, bl);
  ::encode(osd_addrs->hb_back_addr, bl, features);
  ::encode(osd_info, bl);
  ::encode(blacklist, bl, features);
  ::encode(osd_addrs->cluster_addr, bl, features);
  ::encode(cluster_snapshot_epoch, bl);
  ::encode(cluster_snapshot, bl);
  ::encode(*osd_uuid, bl);
  ::encode(osd_xinfo, bl);
  ::encode(osd_addrs->hb_front_addr, bl, features);
}

void OSDMap::encode(bufferlist& bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_OSDMAP_ENC) == 0) {
    encode_classic(bl, features);
    return;
  }

  // only a select set of callers should *ever* be encoding new
  // OSDMaps.  others should be passing around the canonical encoded
  // buffers from on high.  select out those callers by passing in an
  // "impossible" feature bit.
  assert(features & CEPH_FEATURE_RESERVED);
  features &= ~CEPH_FEATURE_RESERVED;

  size_t start_offset = bl.length();
  size_t tail_offset;
  buffer::list::iterator crc_it;

  // meta-encoding: how we include client-used and osd-specific data
  ENCODE_START(8, 7, bl);

  {
    uint8_t v = 4;
    if (!HAVE_FEATURE(features, OSDMAP_REMAP)) {
      v = 3;
    }
    ENCODE_START(v, 1, bl); // client-usable data
    // base
    ::encode(fsid, bl);
    ::encode(epoch, bl);
    ::encode(created, bl);
    ::encode(modified, bl);

    ::encode(pools, bl, features);
    ::encode(pool_name, bl);
    ::encode(pool_max, bl);

    ::encode(flags, bl);

    ::encode(max_osd, bl);
    ::encode(osd_state, bl);
    ::encode(osd_weight, bl);
    ::encode(osd_addrs->client_addr, bl, features);

    ::encode(*pg_temp, bl);
    ::encode(*primary_temp, bl);
    if (osd_primary_affinity) {
      ::encode(*osd_primary_affinity, bl);
    } else {
      vector<__u32> v;
      ::encode(v, bl);
    }

    // crush
    bufferlist cbl;
    crush->encode(cbl, features);
    ::encode(cbl, bl);
    ::encode(erasure_code_profiles, bl);

    if (v >= 4) {
      ::encode(pg_upmap, bl);
      ::encode(pg_upmap_items, bl);
    } else {
      assert(pg_upmap.empty());
      assert(pg_upmap_items.empty());
    }
    ENCODE_FINISH(bl); // client-usable data
  }

  {
    uint8_t target_v = 3;
    if (!HAVE_FEATURE(features, SERVER_LUMINOUS)) {
      target_v = 1;
    }
    ENCODE_START(target_v, 1, bl); // extended, osd-only data
    ::encode(osd_addrs->hb_back_addr, bl, features);
    ::encode(osd_info, bl);
    {
      // put this in a sorted, ordered map<> so that we encode in a
      // deterministic order.
      map<entity_addr_t,utime_t> blacklist_map;
      for (const auto &addr : blacklist)
	blacklist_map.insert(make_pair(addr.first, addr.second));
      ::encode(blacklist_map, bl, features);
    }
    ::encode(osd_addrs->cluster_addr, bl, features);
    ::encode(cluster_snapshot_epoch, bl);
    ::encode(cluster_snapshot, bl);
    ::encode(*osd_uuid, bl);
    ::encode(osd_xinfo, bl);
    ::encode(osd_addrs->hb_front_addr, bl, features);
    if (target_v >= 2) {
      ::encode(nearfull_ratio, bl);
      ::encode(full_ratio, bl);
      ::encode(backfillfull_ratio, bl);
    }
    ENCODE_FINISH(bl); // osd-only data
  }

  ::encode((uint32_t)0, bl); // dummy crc
  crc_it = bl.end();
  crc_it.advance(-4);
  tail_offset = bl.length();

  ENCODE_FINISH(bl); // meta-encoding wrapper

  // fill in crc
  bufferlist front;
  front.substr_of(bl, start_offset, crc_it.get_off() - start_offset);
  crc = front.crc32c(-1);
  if (tail_offset < bl.length()) {
    bufferlist tail;
    tail.substr_of(bl, tail_offset, bl.length() - tail_offset);
    crc = tail.crc32c(crc);
  }
  ceph_le32 crc_le;
  crc_le = crc;
  crc_it.copy_in(4, (char*)&crc_le);
  crc_defined = true;
}

void OSDMap::decode(bufferlist& bl)
{
  auto p = bl.begin();
  decode(p);
}

void OSDMap::decode_classic(bufferlist::iterator& p)
{
  __u32 n, t;
  __u16 v;
  ::decode(v, p);

  // base
  ::decode(fsid, p);
  ::decode(epoch, p);
  ::decode(created, p);
  ::decode(modified, p);

  if (v < 6) {
    if (v < 4) {
      int32_t max_pools = 0;
      ::decode(max_pools, p);
      pool_max = max_pools;
    }
    pools.clear();
    ::decode(n, p);
    while (n--) {
      ::decode(t, p);
      ::decode(pools[t], p);
    }
    if (v == 4) {
      ::decode(n, p);
      pool_max = n;
    } else if (v == 5) {
      pool_name.clear();
      ::decode(n, p);
      while (n--) {
	::decode(t, p);
	::decode(pool_name[t], p);
      }
      ::decode(n, p);
      pool_max = n;
    }
  } else {
    ::decode(pools, p);
    ::decode(pool_name, p);
    ::decode(pool_max, p);
  }
  // kludge around some old bug that zeroed out pool_max (#2307)
  if (pools.size() && pool_max < pools.rbegin()->first) {
    pool_max = pools.rbegin()->first;
  }

  ::decode(flags, p);

  ::decode(max_osd, p);
  ::decode(osd_state, p);
  ::decode(osd_weight, p);
  ::decode(osd_addrs->client_addr, p);
  if (v <= 5) {
    pg_temp->clear();
    ::decode(n, p);
    while (n--) {
      old_pg_t opg;
      ::decode_raw(opg, p);
      ::decode((*pg_temp)[pg_t(opg)], p);
    }
  } else {
    ::decode(*pg_temp, p);
  }

  // crush
  bufferlist cbl;
  ::decode(cbl, p);
  auto cblp = cbl.begin();
  crush->decode(cblp);

  // extended
  __u16 ev = 0;
  if (v >= 5)
    ::decode(ev, p);
  ::decode(osd_addrs->hb_back_addr, p);
  ::decode(osd_info, p);
  if (v < 5)
    ::decode(pool_name, p);

  ::decode(blacklist, p);
  if (ev >= 6)
    ::decode(osd_addrs->cluster_addr, p);
  else
    osd_addrs->cluster_addr.resize(osd_addrs->client_addr.size());

  if (ev >= 7) {
    ::decode(cluster_snapshot_epoch, p);
    ::decode(cluster_snapshot, p);
  }

  if (ev >= 8) {
    ::decode(*osd_uuid, p);
  } else {
    osd_uuid->resize(max_osd);
  }
  if (ev >= 9)
    ::decode(osd_xinfo, p);
  else
    osd_xinfo.resize(max_osd);

  if (ev >= 10)
    ::decode(osd_addrs->hb_front_addr, p);
  else
    osd_addrs->hb_front_addr.resize(osd_addrs->hb_back_addr.size());

  osd_primary_affinity.reset();

  post_decode();
}

void OSDMap::decode(bufferlist::iterator& bl)
{
  /**
   * Older encodings of the OSDMap had a single struct_v which
   * covered the whole encoding, and was prior to our modern
   * stuff which includes a compatv and a size. So if we see
   * a struct_v < 7, we must rewind to the beginning and use our
   * classic decoder.
   */
  size_t start_offset = bl.get_off();
  size_t tail_offset = 0;
  bufferlist crc_front, crc_tail;

  DECODE_START_LEGACY_COMPAT_LEN(8, 7, 7, bl); // wrapper
  if (struct_v < 7) {
    int struct_v_size = sizeof(struct_v);
    bl.advance(-struct_v_size);
    decode_classic(bl);
    return;
  }
  /**
   * Since we made it past that hurdle, we can use our normal paths.
   */
  {
    DECODE_START(4, bl); // client-usable data
    // base
    ::decode(fsid, bl);
    ::decode(epoch, bl);
    ::decode(created, bl);
    ::decode(modified, bl);

    ::decode(pools, bl);
    ::decode(pool_name, bl);
    ::decode(pool_max, bl);

    ::decode(flags, bl);

    ::decode(max_osd, bl);
    ::decode(osd_state, bl);
    ::decode(osd_weight, bl);
    ::decode(osd_addrs->client_addr, bl);

    ::decode(*pg_temp, bl);
    ::decode(*primary_temp, bl);
    if (struct_v >= 2) {
      osd_primary_affinity.reset(new vector<__u32>);
      ::decode(*osd_primary_affinity, bl);
      if (osd_primary_affinity->empty())
	osd_primary_affinity.reset();
    } else {
      osd_primary_affinity.reset();
    }

    // crush
    bufferlist cbl;
    ::decode(cbl, bl);
    auto cblp = cbl.begin();
    crush->decode(cblp);
    if (struct_v >= 3) {
      ::decode(erasure_code_profiles, bl);
    } else {
      erasure_code_profiles.clear();
    }
    if (struct_v >= 4) {
      ::decode(pg_upmap, bl);
      ::decode(pg_upmap_items, bl);
    } else {
      pg_upmap.clear();
      pg_upmap_items.clear();
    }
    DECODE_FINISH(bl); // client-usable data
  }

  {
    DECODE_START(3, bl); // extended, osd-only data
    ::decode(osd_addrs->hb_back_addr, bl);
    ::decode(osd_info, bl);
    ::decode(blacklist, bl);
    ::decode(osd_addrs->cluster_addr, bl);
    ::decode(cluster_snapshot_epoch, bl);
    ::decode(cluster_snapshot, bl);
    ::decode(*osd_uuid, bl);
    ::decode(osd_xinfo, bl);
    ::decode(osd_addrs->hb_front_addr, bl);
    if (struct_v >= 2) {
      ::decode(nearfull_ratio, bl);
      ::decode(full_ratio, bl);
    } else {
      nearfull_ratio = 0;
      full_ratio = 0;
    }
    if (struct_v >= 3) {
      ::decode(backfillfull_ratio, bl);
    } else {
      backfillfull_ratio = 0;
    }
    DECODE_FINISH(bl); // osd-only data
  }

  if (struct_v >= 8) {
    crc_front.substr_of(bl.get_bl(), start_offset, bl.get_off() - start_offset);
    ::decode(crc, bl);
    tail_offset = bl.get_off();
    crc_defined = true;
  } else {
    crc_defined = false;
    crc = 0;
  }

  DECODE_FINISH(bl); // wrapper

  if (tail_offset) {
    // verify crc
    uint32_t actual = crc_front.crc32c(-1);
    if (tail_offset < bl.get_off()) {
      bufferlist tail;
      tail.substr_of(bl.get_bl(), tail_offset, bl.get_off() - tail_offset);
      actual = tail.crc32c(actual);
    }
    if (crc != actual) {
      ostringstream ss;
      ss << "bad crc, actual " << actual << " != expected " << crc;
      string s = ss.str();
      throw buffer::malformed_input(s.c_str());
    }
  }

  post_decode();
}

void OSDMap::post_decode()
{
  // index pool names
  name_pool.clear();
  for (const auto &pname : pool_name) {
    name_pool[pname.second] = pname.first;
  }

  calc_num_osds();
  _calc_up_osd_features();
}

void OSDMap::dump_erasure_code_profiles(const map<string,map<string,string> > &profiles,
					Formatter *f)
{
  f->open_object_section("erasure_code_profiles");
  for (const auto &profile : profiles) {
    f->open_object_section(profile.first.c_str());
    for (const auto &profm : profile.second) {
      f->dump_string(profm.first.c_str(), profm.second.c_str());
    }
    f->close_section();
  }
  f->close_section();
}

void OSDMap::dump(Formatter *f) const
{
  f->dump_int("epoch", get_epoch());
  f->dump_stream("fsid") << get_fsid();
  f->dump_stream("created") << get_created();
  f->dump_stream("modified") << get_modified();
  f->dump_string("flags", get_flag_string());
  f->dump_float("full_ratio", full_ratio);
  f->dump_float("backfillfull_ratio", backfillfull_ratio);
  f->dump_float("nearfull_ratio", nearfull_ratio);
  f->dump_string("cluster_snapshot", get_cluster_snapshot());
  f->dump_int("pool_max", get_pool_max());
  f->dump_int("max_osd", get_max_osd());

  f->open_array_section("pools");
  for (const auto &pool : pools) {
    std::string name("<unknown>");
    const auto &pni = pool_name.find(pool.first);
    if (pni != pool_name.end())
      name = pni->second;
    f->open_object_section("pool");
    f->dump_int("pool", pool.first);
    f->dump_string("pool_name", name);
    pool.second.dump(f);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("osds");
  for (int i=0; i<get_max_osd(); i++)
    if (exists(i)) {
      f->open_object_section("osd_info");
      f->dump_int("osd", i);
      f->dump_stream("uuid") << get_uuid(i);
      f->dump_int("up", is_up(i));
      f->dump_int("in", is_in(i));
      f->dump_float("weight", get_weightf(i));
      f->dump_float("primary_affinity", get_primary_affinityf(i));
      get_info(i).dump(f);
      f->dump_stream("public_addr") << get_addr(i);
      f->dump_stream("cluster_addr") << get_cluster_addr(i);
      f->dump_stream("heartbeat_back_addr") << get_hb_back_addr(i);
      f->dump_stream("heartbeat_front_addr") << get_hb_front_addr(i);

      set<string> st;
      get_state(i, st);
      f->open_array_section("state");
      for (const auto &state : st)
	f->dump_string("state", state);
      f->close_section();

      f->close_section();
    }
  f->close_section();

  f->open_array_section("osd_xinfo");
  for (int i=0; i<get_max_osd(); i++) {
    if (exists(i)) {
      f->open_object_section("xinfo");
      f->dump_int("osd", i);
      osd_xinfo[i].dump(f);
      f->close_section();
    }
  }
  f->close_section();

  f->open_array_section("pg_upmap");
  for (auto& p : pg_upmap) {
    f->open_object_section("mapping");
    f->dump_stream("pgid") << p.first;
    f->open_array_section("osds");
    for (auto q : p.second) {
      f->dump_int("osd", q);
    }
    f->close_section();
    f->close_section();
  }
  f->close_section();
  f->open_array_section("pg_upmap_items");
  for (auto& p : pg_upmap_items) {
    f->open_object_section("mapping");
    f->dump_stream("pgid") << p.first;
    f->open_array_section("mappings");
    for (auto& q : p.second) {
      f->open_object_section("mapping");
      f->dump_int("from", q.first);
      f->dump_int("to", q.second);
      f->close_section();
    }
    f->close_section();
    f->close_section();
  }
  f->close_section();
  f->open_array_section("pg_temp");
  for (const auto &pg : *pg_temp) {
    f->open_object_section("osds");
    f->dump_stream("pgid") << pg.first;
    f->open_array_section("osds");
    for (const auto osd : pg.second)
      f->dump_int("osd", osd);
    f->close_section();
    f->close_section();
  }
  f->close_section();

  f->open_array_section("primary_temp");
  for (const auto &pg : *primary_temp) {
    f->dump_stream("pgid") << pg.first;
    f->dump_int("osd", pg.second);
  }
  f->close_section(); // primary_temp

  f->open_object_section("blacklist");
  for (const auto &addr : blacklist) {
    stringstream ss;
    ss << addr.first;
    f->dump_stream(ss.str().c_str()) << addr.second;
  }
  f->close_section();

  dump_erasure_code_profiles(erasure_code_profiles, f);
}

void OSDMap::generate_test_instances(list<OSDMap*>& o)
{
  o.push_back(new OSDMap);

  CephContext *cct = new CephContext(CODE_ENVIRONMENT_UTILITY);
  o.push_back(new OSDMap);
  uuid_d fsid;
  o.back()->build_simple(cct, 1, fsid, 16, 7, 8);
  o.back()->created = o.back()->modified = utime_t(1, 2);  // fix timestamp
  o.back()->blacklist[entity_addr_t()] = utime_t(5, 6);
  cct->put();
}

string OSDMap::get_flag_string(unsigned f)
{
  string s;
  if ( f& CEPH_OSDMAP_NEARFULL)
    s += ",nearfull";
  if (f & CEPH_OSDMAP_FULL)
    s += ",full";
  if (f & CEPH_OSDMAP_PAUSERD)
    s += ",pauserd";
  if (f & CEPH_OSDMAP_PAUSEWR)
    s += ",pausewr";
  if (f & CEPH_OSDMAP_PAUSEREC)
    s += ",pauserec";
  if (f & CEPH_OSDMAP_NOUP)
    s += ",noup";
  if (f & CEPH_OSDMAP_NODOWN)
    s += ",nodown";
  if (f & CEPH_OSDMAP_NOOUT)
    s += ",noout";
  if (f & CEPH_OSDMAP_NOIN)
    s += ",noin";
  if (f & CEPH_OSDMAP_NOBACKFILL)
    s += ",nobackfill";
  if (f & CEPH_OSDMAP_NOREBALANCE)
    s += ",norebalance";
  if (f & CEPH_OSDMAP_NORECOVER)
    s += ",norecover";
  if (f & CEPH_OSDMAP_NOSCRUB)
    s += ",noscrub";
  if (f & CEPH_OSDMAP_NODEEP_SCRUB)
    s += ",nodeep-scrub";
  if (f & CEPH_OSDMAP_NOTIERAGENT)
    s += ",notieragent";
  if (f & CEPH_OSDMAP_SORTBITWISE)
    s += ",sortbitwise";
  if (f & CEPH_OSDMAP_REQUIRE_JEWEL)
    s += ",require_jewel_osds";
  if (f & CEPH_OSDMAP_REQUIRE_KRAKEN)
    s += ",require_kraken_osds";
  if (f & CEPH_OSDMAP_REQUIRE_LUMINOUS)
    s += ",require_luminous_osds";
  if (s.length())
    s.erase(0, 1);
  return s;
}

string OSDMap::get_flag_string() const
{
  return get_flag_string(flags);
}

struct qi {
  int item;
  int depth;
  float weight;
  qi() : item(0), depth(0), weight(0) {}
  qi(int i, int d, float w) : item(i), depth(d), weight(w) {}
};

void OSDMap::print_pools(ostream& out) const
{
  for (const auto &pool : pools) {
    std::string name("<unknown>");
    const auto &pni = pool_name.find(pool.first);
    if (pni != pool_name.end())
      name = pni->second;
    out << "pool " << pool.first
	<< " '" << name
	<< "' " << pool.second << "\n";

    for (const auto &snap : pool.second.snaps)
      out << "\tsnap " << snap.second.snapid << " '" << snap.second.name << "' " << snap.second.stamp << "\n";

    if (!pool.second.removed_snaps.empty())
      out << "\tremoved_snaps " << pool.second.removed_snaps << "\n";
  }
  out << std::endl;
}

void OSDMap::print(ostream& out) const
{
  out << "epoch " << get_epoch() << "\n"
      << "fsid " << get_fsid() << "\n"
      << "created " << get_created() << "\n"
      << "modified " << get_modified() << "\n";

  out << "flags " << get_flag_string() << "\n";
  out << "full_ratio " << full_ratio << "\n";
  out << "backfillfull_ratio " << backfillfull_ratio << "\n";
  out << "nearfull_ratio " << nearfull_ratio << "\n";
  if (get_cluster_snapshot().length())
    out << "cluster_snapshot " << get_cluster_snapshot() << "\n";
  out << "\n";

  print_pools(out);

  out << "max_osd " << get_max_osd() << "\n";
  for (int i=0; i<get_max_osd(); i++) {
    if (exists(i)) {
      out << "osd." << i;
      out << (is_up(i) ? " up  ":" down");
      out << (is_in(i) ? " in ":" out");
      out << " weight " << get_weightf(i);
      if (get_primary_affinity(i) != CEPH_OSD_DEFAULT_PRIMARY_AFFINITY)
	out << " primary_affinity " << get_primary_affinityf(i);
      const osd_info_t& info(get_info(i));
      out << " " << info;
      out << " " << get_addr(i) << " " << get_cluster_addr(i) << " " << get_hb_back_addr(i)
	  << " " << get_hb_front_addr(i);
      set<string> st;
      get_state(i, st);
      out << " " << st;
      if (!get_uuid(i).is_zero())
	out << " " << get_uuid(i);
      out << "\n";
    }
  }
  out << std::endl;

  for (auto& p : pg_upmap) {
    out << "pg_upmap " << p.first << " " << p.second << "\n";
  }
  for (auto& p : pg_upmap_items) {
    out << "pg_upmap_items " << p.first << " " << p.second << "\n";
  }

  for (const auto pg : *pg_temp)
    out << "pg_temp " << pg.first << " " << pg.second << "\n";

  for (const auto pg : *primary_temp)
    out << "primary_temp " << pg.first << " " << pg.second << "\n";

  for (const auto &addr : blacklist)
    out << "blacklist " << addr.first << " expires " << addr.second << "\n";

  // ignore pg_swap_primary
}

class OSDTreePlainDumper : public CrushTreeDumper::Dumper<TextTable> {
public:
  typedef CrushTreeDumper::Dumper<TextTable> Parent;
  OSDTreePlainDumper(const CrushWrapper *crush, const OSDMap *osdmap_)
    : Parent(crush), osdmap(osdmap_) {}

  void dump(TextTable *tbl) {
    tbl->define_column("ID", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("WEIGHT", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("TYPE NAME", TextTable::LEFT, TextTable::LEFT);
    tbl->define_column("UP/DOWN", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("REWEIGHT", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("PRIMARY-AFFINITY", TextTable::LEFT, TextTable::RIGHT);

    Parent::dump(tbl);

    for (int i = 0; i < osdmap->get_max_osd(); i++) {
      if (osdmap->exists(i) && !is_touched(i))
	dump_item(CrushTreeDumper::Item(i, 0, 0), tbl);
    }
  }

protected:
  void dump_item(const CrushTreeDumper::Item &qi, TextTable *tbl) override {

    *tbl << qi.id
	 << weightf_t(qi.weight);

    ostringstream name;
    for (int k = 0; k < qi.depth; k++)
      name << "    ";
    if (qi.is_bucket()) {
      name << crush->get_type_name(crush->get_bucket_type(qi.id)) << " "
	   << crush->get_item_name(qi.id);
    } else {
      name << "osd." << qi.id;
    }
    *tbl << name.str();

    if (!qi.is_bucket()) {
      if (!osdmap->exists(qi.id)) {
	*tbl << "DNE"
	     << 0;
      } else {
	*tbl << (osdmap->is_up(qi.id) ? "up" : "down")
	     << weightf_t(osdmap->get_weightf(qi.id))
	     << weightf_t(osdmap->get_primary_affinityf(qi.id));
      }
    }
    *tbl << TextTable::endrow;
  }

private:
  const OSDMap *osdmap;
};

class OSDTreeFormattingDumper : public CrushTreeDumper::FormattingDumper {
public:
  typedef CrushTreeDumper::FormattingDumper Parent;

  OSDTreeFormattingDumper(const CrushWrapper *crush, const OSDMap *osdmap_)
    : Parent(crush), osdmap(osdmap_) {}

  void dump(Formatter *f) {
    f->open_array_section("nodes");
    Parent::dump(f);
    f->close_section();
    f->open_array_section("stray");
    for (int i = 0; i < osdmap->get_max_osd(); i++) {
      if (osdmap->exists(i) && !is_touched(i))
	dump_item(CrushTreeDumper::Item(i, 0, 0), f);
    }
    f->close_section();
  }

protected:
  void dump_item_fields(const CrushTreeDumper::Item &qi, Formatter *f) override {
    Parent::dump_item_fields(qi, f);
    if (!qi.is_bucket())
    {
      f->dump_unsigned("exists", (int)osdmap->exists(qi.id));
      f->dump_string("status", osdmap->is_up(qi.id) ? "up" : "down");
      f->dump_float("reweight", osdmap->get_weightf(qi.id));
      f->dump_float("primary_affinity", osdmap->get_primary_affinityf(qi.id));
    }
  }

private:
  const OSDMap *osdmap;
};

void OSDMap::print_tree(Formatter *f, ostream *out) const
{
  if (f)
    OSDTreeFormattingDumper(crush.get(), this).dump(f);
  else {
    assert(out);
    TextTable tbl;
    OSDTreePlainDumper(crush.get(), this).dump(&tbl);
    *out << tbl;
  }
}

void OSDMap::print_summary(Formatter *f, ostream& out) const
{
  if (f) {
    f->open_object_section("osdmap");
    f->dump_int("epoch", get_epoch());
    f->dump_int("num_osds", get_num_osds());
    f->dump_int("num_up_osds", get_num_up_osds());
    f->dump_int("num_in_osds", get_num_in_osds());
    f->dump_bool("full", test_flag(CEPH_OSDMAP_FULL) ? true : false);
    f->dump_bool("nearfull", test_flag(CEPH_OSDMAP_NEARFULL) ? true : false);
    f->dump_unsigned("num_remapped_pgs", get_num_pg_temp());
    f->close_section();
  } else {
    out << "     osdmap e" << get_epoch() << ": "
	<< get_num_osds() << " osds: "
	<< get_num_up_osds() << " up, "
	<< get_num_in_osds() << " in";
    if (get_num_pg_temp())
      out << "; " << get_num_pg_temp() << " remapped pgs";
    out << "\n";
    uint64_t important_flags = flags & ~CEPH_OSDMAP_SEMIHIDDEN_FLAGS;
    if (important_flags)
      out << "            flags " << get_flag_string(important_flags) << "\n";
  }
}

void OSDMap::print_oneline_summary(ostream& out) const
{
  out << "e" << get_epoch() << ": "
      << get_num_osds() << " osds: "
      << get_num_up_osds() << " up, "
      << get_num_in_osds() << " in";
  if (test_flag(CEPH_OSDMAP_FULL))
    out << " full";
  else if (test_flag(CEPH_OSDMAP_NEARFULL))
    out << " nearfull";
}

bool OSDMap::crush_ruleset_in_use(int ruleset) const
{
  for (const auto &pool : pools) {
    if (pool.second.crush_ruleset == ruleset)
      return true;
  }
  return false;
}

int OSDMap::build_simple(CephContext *cct, epoch_t e, uuid_d &fsid,
			  int nosd, int pg_bits, int pgp_bits)
{
  ldout(cct, 10) << "build_simple on " << num_osd
		 << " osds with " << pg_bits << " pg bits per osd, "
		 << dendl;
  epoch = e;
  set_fsid(fsid);
  created = modified = ceph_clock_now();

  if (nosd >=  0) {
    set_max_osd(nosd);
  } else {
    // count osds
    int maxosd = 0;
    const md_config_t *conf = cct->_conf;
    vector<string> sections;
    conf->get_all_sections(sections);

    for (auto &section : sections) {
      if (section.find("osd.") != 0)
	continue;

      const char *begin = section.c_str() + 4;
      char *end = (char*)begin;
      int o = strtol(begin, &end, 10);
      if (*end != '\0')
	continue;

      if (o > cct->_conf->mon_max_osd) {
	lderr(cct) << "[osd." << o << "] in config has id > mon_max_osd " << cct->_conf->mon_max_osd << dendl;
	return -ERANGE;
      }

      if (o > maxosd)
	maxosd = o;
    }

    set_max_osd(maxosd + 1);
  }

  // pgp_num <= pg_num
  if (pgp_bits > pg_bits)
    pgp_bits = pg_bits;

  vector<string> pool_names;
  pool_names.push_back("rbd");

  stringstream ss;
  int r;
  if (nosd >= 0)
    r = build_simple_crush_map(cct, *crush, nosd, &ss);
  else
    r = build_simple_crush_map_from_conf(cct, *crush, &ss);
  assert(r == 0);

  int poolbase = get_max_osd() ? get_max_osd() : 1;

  int const default_replicated_ruleset = crush->get_osd_pool_default_crush_replicated_ruleset(cct);
  assert(default_replicated_ruleset >= 0);

  for (auto &plname : pool_names) {
    int64_t pool = ++pool_max;
    pools[pool].type = pg_pool_t::TYPE_REPLICATED;
    pools[pool].flags = cct->_conf->osd_pool_default_flags;
    if (cct->_conf->osd_pool_default_flag_hashpspool)
      pools[pool].set_flag(pg_pool_t::FLAG_HASHPSPOOL);
    if (cct->_conf->osd_pool_default_flag_nodelete)
      pools[pool].set_flag(pg_pool_t::FLAG_NODELETE);
    if (cct->_conf->osd_pool_default_flag_nopgchange)
      pools[pool].set_flag(pg_pool_t::FLAG_NOPGCHANGE);
    if (cct->_conf->osd_pool_default_flag_nosizechange)
      pools[pool].set_flag(pg_pool_t::FLAG_NOSIZECHANGE);
    pools[pool].size = cct->_conf->osd_pool_default_size;
    pools[pool].min_size = cct->_conf->get_osd_pool_default_min_size();
    pools[pool].crush_ruleset = default_replicated_ruleset;
    pools[pool].object_hash = CEPH_STR_HASH_RJENKINS;
    pools[pool].set_pg_num(poolbase << pg_bits);
    pools[pool].set_pgp_num(poolbase << pgp_bits);
    pools[pool].last_change = epoch;
    pool_name[pool] = plname;
    name_pool[plname] = pool;
  }

  for (int i=0; i<get_max_osd(); i++) {
    set_state(i, 0);
    set_weight(i, CEPH_OSD_OUT);
  }

  map<string,string> profile_map;
  r = get_erasure_code_profile_default(cct, profile_map, &ss);
  if (r < 0) {
    lderr(cct) << ss.str() << dendl;
    return r;
  }
  set_erasure_code_profile("default", profile_map);
  return 0;
}

int OSDMap::get_erasure_code_profile_default(CephContext *cct,
					     map<string,string> &profile_map,
					     ostream *ss)
{
  int r = get_json_str_map(cct->_conf->osd_pool_default_erasure_code_profile,
		      *ss,
		      &profile_map);
  return r;
}

int OSDMap::_build_crush_types(CrushWrapper& crush)
{
  crush.set_type_name(0, "osd");
  crush.set_type_name(1, "host");
  crush.set_type_name(2, "chassis");
  crush.set_type_name(3, "rack");
  crush.set_type_name(4, "row");
  crush.set_type_name(5, "pdu");
  crush.set_type_name(6, "pod");
  crush.set_type_name(7, "room");
  crush.set_type_name(8, "datacenter");
  crush.set_type_name(9, "region");
  crush.set_type_name(10, "root");
  return 10;
}

int OSDMap::build_simple_crush_map(CephContext *cct, CrushWrapper& crush,
				   int nosd, ostream *ss)
{
  crush.create();

  // root
  int root_type = _build_crush_types(crush);
  int rootid;
  int r = crush.add_bucket(0, 0, CRUSH_HASH_DEFAULT,
			   root_type, 0, NULL, NULL, &rootid);
  assert(r == 0);
  crush.set_item_name(rootid, "default");

  for (int o=0; o<nosd; o++) {
    map<string,string> loc;
    loc["host"] = "localhost";
    loc["rack"] = "localrack";
    loc["root"] = "default";
    ldout(cct, 10) << " adding osd." << o << " at " << loc << dendl;
    char name[32];
    snprintf(name, sizeof(name), "osd.%d", o);
    crush.insert_item(cct, o, 1.0, name, loc);
  }

  build_simple_crush_rulesets(cct, crush, "default", ss);

  crush.finalize();

  return 0;
}

int OSDMap::build_simple_crush_map_from_conf(CephContext *cct,
					     CrushWrapper& crush,
					     ostream *ss)
{
  const md_config_t *conf = cct->_conf;

  crush.create();

  // root
  int root_type = _build_crush_types(crush);
  int rootid;
  int r = crush.add_bucket(0, 0,
			   CRUSH_HASH_DEFAULT,
			   root_type, 0, NULL, NULL, &rootid);
  assert(r == 0);
  crush.set_item_name(rootid, "default");

  // add osds
  vector<string> sections;
  conf->get_all_sections(sections);

  for (auto &section : sections) {
    if (section.find("osd.") != 0)
      continue;

    const char *begin = section.c_str() + 4;
    char *end = (char*)begin;
    int o = strtol(begin, &end, 10);
    if (*end != '\0')
      continue;

    string host, rack, row, room, dc, pool;
    vector<string> sectiontmp;
    sectiontmp.push_back("osd");
    sectiontmp.push_back(section);
    conf->get_val_from_conf_file(sectiontmp, "host", host, false);
    conf->get_val_from_conf_file(sectiontmp, "rack", rack, false);
    conf->get_val_from_conf_file(sectiontmp, "row", row, false);
    conf->get_val_from_conf_file(sectiontmp, "room", room, false);
    conf->get_val_from_conf_file(sectiontmp, "datacenter", dc, false);
    conf->get_val_from_conf_file(sectiontmp, "root", pool, false);

    if (host.length() == 0)
      host = "unknownhost";
    if (rack.length() == 0)
      rack = "unknownrack";

    map<string,string> loc;
    loc["host"] = host;
    loc["rack"] = rack;
    if (row.size())
      loc["row"] = row;
    if (room.size())
      loc["room"] = room;
    if (dc.size())
      loc["datacenter"] = dc;
    loc["root"] = "default";

    ldout(cct, 5) << " adding osd." << o << " at " << loc << dendl;
    crush.insert_item(cct, o, 1.0, section, loc);
  }

  build_simple_crush_rulesets(cct, crush, "default", ss);

  crush.finalize();

  return 0;
}


int OSDMap::build_simple_crush_rulesets(CephContext *cct,
					CrushWrapper& crush,
					const string& root,
					ostream *ss)
{
  int crush_ruleset =
      crush._get_osd_pool_default_crush_replicated_ruleset(cct, true);
  string failure_domain =
    crush.get_type_name(cct->_conf->osd_crush_chooseleaf_type);

  if (crush_ruleset == CEPH_DEFAULT_CRUSH_REPLICATED_RULESET)
    crush_ruleset = -1; // create ruleset 0 by default

  int r;
  r = crush.add_simple_ruleset_at("replicated_ruleset", root, failure_domain,
                                  "firstn", pg_pool_t::TYPE_REPLICATED,
                                  crush_ruleset, ss);
  if (r < 0)
    return r;
  // do not add an erasure rule by default or else we will implicitly
  // require the crush_v2 feature of clients
  return 0;
}

int OSDMap::summarize_mapping_stats(
  OSDMap *newmap,
  const set<int64_t> *pools,
  std::string *out,
  Formatter *f) const
{
  set<int64_t> ls;
  if (pools) {
    ls = *pools;
  } else {
    for (auto &p : get_pools())
      ls.insert(p.first);
  }

  unsigned total_pg = 0;
  unsigned moved_pg = 0;
  vector<unsigned> base_by_osd(get_max_osd(), 0);
  vector<unsigned> new_by_osd(get_max_osd(), 0);
  for (int64_t pool_id : ls) {
    const pg_pool_t *pi = get_pg_pool(pool_id);
    vector<int> up, up2, acting;
    int up_primary, acting_primary;
    for (unsigned ps = 0; ps < pi->get_pg_num(); ++ps) {
      pg_t pgid(ps, pool_id, -1);
      total_pg += pi->get_size();
      pg_to_up_acting_osds(pgid, &up, &up_primary,
			   &acting, &acting_primary);
      for (int osd : up) {
	if (osd >= 0 && osd < get_max_osd())
	  ++base_by_osd[osd];
      }
      if (newmap) {
	newmap->pg_to_up_acting_osds(pgid, &up2, &up_primary,
				     &acting, &acting_primary);
	for (int osd : up2) {
	  if (osd >= 0 && osd < get_max_osd())
	    ++new_by_osd[osd];
	}
	if (pi->type == pg_pool_t::TYPE_ERASURE) {
	  for (unsigned i=0; i<up.size(); ++i) {
	    if (up[i] != up2[i]) {
	      ++moved_pg;
	    }
	  }
	} else if (pi->type == pg_pool_t::TYPE_REPLICATED) {
	  for (int osd : up) {
	    if (std::find(up2.begin(), up2.end(), osd) == up2.end()) {
	      ++moved_pg;
	    }
	  }
	} else {
	  assert(0 == "unhandled pool type");
	}
      }
    }
  }

  unsigned num_up_in = 0;
  for (int osd = 0; osd < get_max_osd(); ++osd) {
    if (is_up(osd) && is_in(osd))
      ++num_up_in;
  }
  if (!num_up_in) {
    return -EINVAL;
  }

  float avg_pg = (float)total_pg / (float)num_up_in;
  float base_stddev = 0, new_stddev = 0;
  int min = -1, max = -1;
  unsigned min_base_pg = 0, max_base_pg = 0;
  unsigned min_new_pg = 0, max_new_pg = 0;
  for (int osd = 0; osd < get_max_osd(); ++osd) {
    if (is_up(osd) && is_in(osd)) {
      float base_diff = (float)base_by_osd[osd] - avg_pg;
      base_stddev += base_diff * base_diff;
      float new_diff = (float)new_by_osd[osd] - avg_pg;
      new_stddev += new_diff * new_diff;
      if (min < 0 || base_by_osd[osd] < min_base_pg) {
	min = osd;
	min_base_pg = base_by_osd[osd];
	min_new_pg = new_by_osd[osd];
      }
      if (max < 0 || base_by_osd[osd] > max_base_pg) {
	max = osd;
	max_base_pg = base_by_osd[osd];
	max_new_pg = new_by_osd[osd];
      }
    }
  }
  base_stddev = sqrt(base_stddev / num_up_in);
  new_stddev = sqrt(new_stddev / num_up_in);

  float edev = sqrt(avg_pg * (1.0 - (1.0 / (double)num_up_in)));

  ostringstream ss;
  if (f)
    f->open_object_section("utilization");
  if (newmap) {
    if (f) {
      f->dump_unsigned("moved_pgs", moved_pg);
      f->dump_unsigned("total_pgs", total_pg);
    } else {
      float percent = 0;
      if (total_pg)
        percent = (float)moved_pg * 100.0 / (float)total_pg;
      ss << "moved " << moved_pg << " / " << total_pg
	 << " (" << percent << "%)\n";
    }
  }
  if (f) {
    f->dump_float("avg_pgs", avg_pg);
    f->dump_float("std_dev", base_stddev);
    f->dump_float("expected_baseline_std_dev", edev);
    if (newmap)
      f->dump_float("new_std_dev", new_stddev);
  } else {
    ss << "avg " << avg_pg << "\n";
    ss << "stddev " << base_stddev;
    if (newmap)
      ss << " -> " << new_stddev;
    ss << " (expected baseline " << edev << ")\n";
  }
  if (min >= 0) {
    if (f) {
      f->dump_unsigned("min_osd", min);
      f->dump_unsigned("min_osd_pgs", min_base_pg);
      if (newmap)
	f->dump_unsigned("new_min_osd_pgs", min_new_pg);
    } else {
      ss << "min osd." << min << " with " << min_base_pg;
      if (newmap)
	ss << " -> " << min_new_pg;
      ss << " pgs (" << (float)min_base_pg / avg_pg;
      if (newmap)
	ss << " -> " << (float)min_new_pg / avg_pg;
      ss << " * mean)\n";
    }
  }
  if (max >= 0) {
    if (f) {
      f->dump_unsigned("max_osd", max);
      f->dump_unsigned("max_osd_pgs", max_base_pg);
      if (newmap)
	f->dump_unsigned("new_max_osd_pgs", max_new_pg);
    } else {
      ss << "max osd." << max << " with " << max_base_pg;
      if (newmap)
	ss << " -> " << max_new_pg;
      ss << " pgs (" << (float)max_base_pg / avg_pg;
      if (newmap)
	ss << " -> " << (float)max_new_pg / avg_pg;
      ss << " * mean)\n";
    }
  }
  if (f)
    f->close_section();
  if (out)
    *out = ss.str();
  return 0;
}


int OSDMap::clean_pg_upmaps(
  CephContext *cct,
  Incremental *pending_inc)
{
  ldout(cct, 10) << __func__ << dendl;
  int changed = 0;
  for (auto& p : pg_upmap) {
    vector<int> raw;
    int primary;
    pg_to_raw_osds(p.first, &raw, &primary);
    if (raw == p.second) {
      ldout(cct, 10) << " removing redundant pg_upmap " << p.first << " "
		     << p.second << dendl;
      pending_inc->old_pg_upmap.insert(p.first);
      ++changed;
    }
  }
  for (auto& p : pg_upmap_items) {
    vector<int> raw;
    int primary;
    pg_to_raw_osds(p.first, &raw, &primary);
    vector<pair<int,int>> newmap;
    for (auto& q : p.second) {
      if (std::find(raw.begin(), raw.end(), q.first) != raw.end()) {
	newmap.push_back(q);
      }
    }
    if (newmap.empty()) {
      ldout(cct, 10) << " removing no-op pg_upmap_items " << p.first << " "
		     << p.second << dendl;
      pending_inc->old_pg_upmap_items.insert(p.first);
      ++changed;
    } else if (newmap != p.second) {
      ldout(cct, 10) << " simplifying partially no-op pg_upmap_items "
		     << p.first << " " << p.second << " -> " << newmap << dendl;
      pending_inc->new_pg_upmap_items[p.first] = newmap;
      ++changed;
    }
  }
  return changed;
}

bool OSDMap::try_pg_upmap(
  CephContext *cct,
  pg_t pg,                       ///< pg to potentially remap
  const set<int>& overfull,      ///< osds we'd want to evacuate
  const vector<int>& underfull,  ///< osds to move to, in order of preference
  vector<int> *orig,
  vector<int> *out)              ///< resulting alternative mapping
{
  const pg_pool_t *pool = get_pg_pool(pg.pool());
  if (!pool)
    return false;
  int rule = crush->find_rule(pool->get_crush_ruleset(), pool->get_type(),
			      pool->get_size());
  if (rule < 0)
    return false;

  // get original mapping
  _pg_to_raw_osds(*pool, pg, orig, NULL);

  // make sure there is something there to remap
  bool any = false;
  for (auto osd : *orig) {
    if (overfull.count(osd)) {
      any = true;
      break;
    }
  }
  if (!any) {
    return false;
  }

  int r = crush->try_remap_rule(
    cct,
    rule,
    pool->get_size(),
    overfull, underfull,
    *orig,
    out);
  if (r < 0)
    return false;
  if (*out == *orig)
    return false;
  return true;
}

int OSDMap::calc_pg_upmaps(
  CephContext *cct,
  float max_deviation,
  int max,
  const set<int64_t>& only_pools,
  OSDMap::Incremental *pending_inc)
{
  OSDMap tmp;
  tmp.deepish_copy_from(*this);
  int num_changed = 0;
  while (true) {
    map<int,set<pg_t>> pgs_by_osd;
    int total_pgs = 0;
    for (auto& i : pools) {
      if (!only_pools.empty() && !only_pools.count(i.first))
	continue;
      for (unsigned ps = 0; ps < i.second.get_pg_num(); ++ps) {
	pg_t pg(ps, i.first);
	vector<int> up;
	tmp.pg_to_up_acting_osds(pg, &up, nullptr, nullptr, nullptr);
	for (auto osd : up) {
	  if (osd != CRUSH_ITEM_NONE)
	    pgs_by_osd[osd].insert(pg);
	}
      }
      total_pgs += i.second.get_size() * i.second.get_pg_num();
    }
    float osd_weight_total = 0;
    map<int,float> osd_weight;
    for (auto& i : pgs_by_osd) {
      float w = crush->get_item_weightf(i.first);
      osd_weight[i.first] = w;
      osd_weight_total += w;
      ldout(cct, 20) << " osd." << i.first << " weight " << w
		     << " pgs " << i.second.size() << dendl;
    }

    // NOTE: we assume we touch all osds with CRUSH!
    float pgs_per_weight = total_pgs / osd_weight_total;
    ldout(cct, 10) << " osd_weight_total " << osd_weight_total << dendl;
    ldout(cct, 10) << " pgs_per_weight " << pgs_per_weight << dendl;

    // osd deviation
    map<int,float> osd_deviation;       // osd, deviation(pgs)
    multimap<float,int> deviation_osd;  // deviation(pgs), osd
    set<int> overfull;
    for (auto& i : pgs_by_osd) {
      float target = osd_weight[i.first] * pgs_per_weight;
      float deviation = (float)i.second.size() - target;
      ldout(cct, 20) << " osd." << i.first
		     << "\tpgs " << i.second.size()
		     << "\ttarget " << target
		     << "\tdeviation " << deviation
		     << dendl;
      osd_deviation[i.first] = deviation;
      deviation_osd.insert(make_pair(deviation, i.first));
      if (deviation > 0)
	overfull.insert(i.first);
    }

    // build underfull, sorted from least-full to most-average
    vector<int> underfull;
    for (auto i = deviation_osd.begin();
	 i != deviation_osd.end();
	 ++i) {
      if (i->first >= -.999)
	break;
      underfull.push_back(i->second);
    }
    ldout(cct, 10) << " overfull " << overfull
		   << " underfull " << underfull << dendl;
    if (overfull.empty() || underfull.empty())
      break;

    // pick fullest
    bool restart = false;
    for (auto p = deviation_osd.rbegin(); p != deviation_osd.rend(); ++p) {
      int osd = p->second;
      float target = osd_weight[osd] * pgs_per_weight;
      float deviation = deviation_osd.rbegin()->first;
      if (deviation/target < max_deviation) {
	ldout(cct, 10) << " osd." << osd
		       << " target " << target
		       << " deviation " << deviation
		       << " -> " << deviation/target
		       << " < max " << max_deviation << dendl;
	break;
      }
      int num_to_move = deviation;
      ldout(cct, 10) << " osd." << osd << " move " << num_to_move << dendl;
      if (num_to_move < 1)
	break;

      set<pg_t>& pgs = pgs_by_osd[osd];

      // look for remaps we can un-remap
      for (auto pg : pgs) {
	auto p = tmp.pg_upmap_items.find(pg);
	if (p != tmp.pg_upmap_items.end()) {
	  for (auto q : p->second) {
	    if (q.second == osd) {
	      ldout(cct, 10) << "  dropping pg_upmap_items " << pg
			     << " " << p->second << dendl;
	      tmp.pg_upmap_items.erase(p);
	      pending_inc->old_pg_upmap_items.insert(pg);
	      ++num_changed;
	      restart = true;
	    }
	  }
	}
	if (restart)
	  break;
      } // pg loop
      if (restart)
	break;

      for (auto pg : pgs) {
	if (tmp.pg_upmap.count(pg) ||
	    tmp.pg_upmap_items.count(pg)) {
	  ldout(cct, 20) << "  already remapped " << pg << dendl;
	  continue;
	}
	ldout(cct, 10) << "  trying " << pg << dendl;
	vector<int> orig, out;
	if (!try_pg_upmap(cct, pg, overfull, underfull, &orig, &out)) {
	  continue;
	}
	ldout(cct, 10) << "  " << pg << " " << orig << " -> " << out << dendl;
	if (orig.size() != out.size()) {
	  continue;
	}
	assert(orig != out);
	vector<pair<int,int>>& rmi = tmp.pg_upmap_items[pg];
	for (unsigned i = 0; i < out.size(); ++i) {
	  if (orig[i] != out[i]) {
	    rmi.push_back(make_pair(orig[i], out[i]));
	  }
	}
	pending_inc->new_pg_upmap_items[pg] = rmi;
	ldout(cct, 10) << "  " << pg << " pg_upmap_items " << rmi << dendl;
	restart = true;
	++num_changed;
	break;
      } // pg loop
      if (restart)
	break;
    } // osd loop

    if (!restart) {
      ldout(cct, 10) << " failed to find any changes to make" << dendl;
      break;
    }
    if (--max == 0) {
      ldout(cct, 10) << " hit max iterations, stopping" << dendl;
      break;
    }
  }
  return num_changed;
}
