from collections import defaultdict
from distutils.version import StrictVersion

from django.http import Http404
import rest_framework
from rest_framework.exceptions import ParseError
from rest_framework.response import Response

from rest_framework import status

from rest.app.serializers.v2 import PoolSerializer, CrushRuleSetSerializer, \
    CrushRuleSerializer, ServerSerializer, RequestSerializer, OsdSerializer, \
    ConfigSettingSerializer, MonSerializer, OsdConfigSerializer
from rest.app.views.rpc_view import RPCViewSet, DataObject
from rest.app.types import CRUSH_RULE, POOL, OSD, USER_REQUEST_COMPLETE, \
    USER_REQUEST_SUBMITTED, OSD_IMPLEMENTED_COMMANDS, OSD_MAP, \
    SYNC_OBJECT_TYPES, OsdMap, Config, MonMap, MonStatus, SYNC_OBJECT_STR_TYPE


from rest.logger import logger
log = logger()


class RequestViewSet(RPCViewSet):
    """
Calamari server requests, tracking long-running operations on the Calamari server.  Some
API resources return a ``202 ACCEPTED`` response with a request ID, which you can use with
this resource to learn about progress and completion of an operation.  This resource is
paginated.

May optionally filter by state by passing a ``?state=<state>`` GET parameter, where
state is one of 'complete', 'submitted'.

The returned records are ordered by the 'requested_at' attribute, in descending order (i.e.
the first page of results contains the most recent requests).

To cancel a request while it is running, send an empty POST to ``request/<request id>/cancel``.
    """
    serializer_class = RequestSerializer

    def cancel(self, request, request_id):
        user_request = DataObject(self.client.cancel_request(request_id))
        return Response(self.serializer_class(user_request).data)

    def retrieve(self, request, **kwargs):
        request_id = kwargs['request_id']
        user_request = DataObject(self.client.get_request(request_id))
        return Response(self.serializer_class(user_request).data)

    def list(self, request, **kwargs):
        fsid = kwargs.get('fsid', None)
        filter_state = request.GET.get('state', None)
        valid_states = [USER_REQUEST_COMPLETE, USER_REQUEST_SUBMITTED]
        if filter_state is not None and filter_state not in valid_states:
            raise ParseError("State must be one of %s" % ", ".join(valid_states))

        requests = self.client.list_requests({'state': filter_state, 'fsid': fsid})
        if StrictVersion(rest_framework.__version__) < StrictVersion("3.0.0"):
            return Response(self._paginate(request, requests))
        else:
            # FIXME reinstate pagination, broke in DRF 2.x -> 3.x
            return Response(requests)


class CrushRuleViewSet(RPCViewSet):
    """
A CRUSH ruleset is a collection of CRUSH rules which are applied
together to a pool.
    """
    serializer_class = CrushRuleSerializer

    def list(self, request):
        rules = self.client.list(CRUSH_RULE, {})
        osds_by_rule_id = self.client.get_sync_object(OsdMap, ['osds_by_rule_id'])
        for rule in rules:
            rule['osd_count'] = len(osds_by_rule_id[rule['rule_id']])
        return Response(CrushRuleSerializer([DataObject(r) for r in rules], many=True).data)


class CrushRuleSetViewSet(RPCViewSet):
    """
A CRUSH rule is used by Ceph to decide where to locate placement groups on OSDs.
    """
    serializer_class = CrushRuleSetSerializer

    def list(self, request):
        rules = self.client.list(CRUSH_RULE, {})
        osds_by_rule_id = self.client.get_sync_object(OsdMap, ['osds_by_rule_id'])
        rulesets_data = defaultdict(list)
        for rule in rules:
            rule['osd_count'] = len(osds_by_rule_id[rule['rule_id']])
            rulesets_data[rule['ruleset']].append(rule)

        rulesets = [DataObject({
            'id': rd_id,
            'rules': [DataObject(r) for r in rd_rules]
        }) for (rd_id, rd_rules) in rulesets_data.items()]

        return Response(CrushRuleSetSerializer(rulesets, many=True).data)


class PoolDataObject(DataObject):
    """
    Slightly dressed up version of the raw pool from osd dump
    """

    FLAG_HASHPSPOOL = 1
    FLAG_FULL = 2

    @property
    def hashpspool(self):
        return bool(self.flags & self.FLAG_HASHPSPOOL)

    @property
    def full(self):
        return bool(self.flags & self.FLAG_FULL)


class RequestReturner(object):
    """
    Helper for ViewSets that sometimes need to return a request handle
    """
    def _return_request(self, request):
        if request:
            return Response(request, status=status.HTTP_202_ACCEPTED)
        else:
            return Response(status=status.HTTP_304_NOT_MODIFIED)


class NullableDataObject(DataObject):
    """
    A DataObject which synthesizes Nones for any attributes it doesn't have
    """
    def __getattr__(self, item):
        if not item.startswith('_'):
            return self.__dict__.get(item, None)
        else:
            raise AttributeError


class ConfigViewSet(RPCViewSet):
    """
Configuration settings from a Ceph Cluster.
    """
    serializer_class = ConfigSettingSerializer

    def list(self, request):
        ceph_config = self.client.get_sync_object(Config).data
        settings = [DataObject({'key': k, 'value': v}) for (k, v) in ceph_config.items()]
        return Response(self.serializer_class(settings, many=True).data)

    def retrieve(self, request, key):
        ceph_config = self.client.get_sync_object(Config).data
        try:
            setting = DataObject({'key': key, 'value': ceph_config[key]})
        except KeyError:
            raise Http404("Key '%s' not found" % key)
        else:
            return Response(self.serializer_class(setting).data)


def _config_to_bool(config_val):
    return {'true': True, 'false': False}[config_val.lower()]


class PoolViewSet(RPCViewSet, RequestReturner):
    """
Manage Ceph storage pools.

To get the default values which will be used for any fields omitted from a POST, do
a GET with the ?defaults argument.  The returned pool object will contain all attributes,
but those without static defaults will be set to null.

    """
    serializer_class = PoolSerializer

    def _defaults(self):
        # Issue overlapped RPCs first
        ceph_config = self.client.get_sync_object(Config)
        rules = self.client.list(CRUSH_RULE, {})

        if not ceph_config:
            return Response("Cluster configuration unavailable", status=status.HTTP_503_SERVICE_UNAVAILABLE)

        if not rules:
            return Response("No CRUSH rules exist, pool creation is impossible",
                            status=status.HTTP_503_SERVICE_UNAVAILABLE)

        # Ceph does not reliably inform us of a default ruleset that exists, so we check
        # what it tells us against the rulesets we know about.
        ruleset_ids = sorted(list(set([r['ruleset'] for r in rules])))
        if int(ceph_config['osd_pool_default_crush_rule']) in ruleset_ids:
            # This is the ceph<0.80 setting
            default_ruleset = ceph_config['osd_pool_default_crush_rule']
        elif int(ceph_config.get('osd_pool_default_crush_replicated_ruleset', -1)) in ruleset_ids:
            # This is the ceph>=0.80
            default_ruleset = ceph_config['osd_pool_default_crush_replicated_ruleset']
        else:
            # Ceph may have an invalid default set which
            # would cause undefined behaviour in pool creation (#8373)
            # In this case, pick lowest numbered ruleset as default
            default_ruleset = ruleset_ids[0]

        defaults = NullableDataObject({
            'size': int(ceph_config['osd_pool_default_size']),
            'crush_ruleset': int(default_ruleset),
            'min_size': int(ceph_config['osd_pool_default_min_size']),
            'hashpspool': _config_to_bool(ceph_config['osd_pool_default_flag_hashpspool']),
            # Crash replay interval is zero by default when you create a pool, but when ceph creates
            # its own data pool it applies 'osd_default_data_pool_replay_window'.  If we add UI for adding
            # pools to a filesystem, we should check that those data pools have this set.
            'crash_replay_interval': 0,
            'quota_max_objects': 0,
            'quota_max_bytes': 0
        })

        return Response(PoolSerializer(defaults).data)

    def list(self, request):
        if 'defaults' in request.GET:
            return self._defaults()

        pools = [PoolDataObject(p) for p in self.client.list(POOL, {})]
        return Response(PoolSerializer(pools, many=True).data)

    def retrieve(self, request, pool_id):
        pool = PoolDataObject(self.client.get(POOL, int(pool_id)))
        return Response(PoolSerializer(pool).data)

    def create(self, request):
        serializer = self.serializer_class(data=request.DATA)
        if serializer.is_valid(request.method):
            response = self._validate_semantics(None, serializer.get_data())
            if response is not None:
                return response

            create_response = self.client.create(POOL, serializer.get_data())

            # TODO: handle case where the creation is rejected for some reason (should
            # be passed an errors dict for a clean failure, or a zerorpc exception
            # for a dirty failure)
            assert 'request_id' in create_response
            return Response(create_response, status=status.HTTP_202_ACCEPTED)
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def update(self, request, pool_id):
        serializer = self.serializer_class(data=request.DATA)
        if serializer.is_valid(request.method):
            response = self._validate_semantics(pool_id, serializer.get_data())
            if response is not None:
                return response

            return self._return_request(self.client.update(POOL, int(pool_id), serializer.get_data()))
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def destroy(self, request, pool_id):
        delete_response = self.client.delete(POOL, int(pool_id), status=status.HTTP_202_ACCEPTED)
        return Response(delete_response, status=status.HTTP_202_ACCEPTED)

    def _validate_semantics(self, pool_id, data):
        errors = defaultdict(list)
        self._check_name_unique(data, errors)
        self._check_crush_ruleset(data, errors)
        self._check_pgp_less_than_pg_num(data, errors)
        self._check_pg_nums_dont_decrease(pool_id, data, errors)
        self._check_pg_num_inside_config_bounds(data, errors)

        if errors.items():
            if 'name' in errors:
                return Response(errors, status=status.HTTP_409_CONFLICT)
            else:
                return Response(errors, status=status.HTTP_400_BAD_REQUEST)

    def _check_pg_nums_dont_decrease(self, pool_id, data, errors):
        if pool_id is not None:
            detail = self.client.get(POOL, int(pool_id))
            for field in ['pg_num', 'pgp_num']:
                expanded_field = 'pg_placement_num' if field == 'pgp_num' else 'pg_num'
                if field in data and data[field] < detail[expanded_field]:
                    errors[field].append('must be >= than current {field}'.format(field=field))

    def _check_crush_ruleset(self, data, errors):
        if 'crush_ruleset' in data:
            rules = self.client.list(CRUSH_RULE, {})
            rulesets = set(r['ruleset'] for r in rules)
            if data['crush_ruleset'] not in rulesets:
                errors['crush_ruleset'].append("CRUSH ruleset {0} not found".format(data['crush_ruleset']))

    def _check_pg_num_inside_config_bounds(self, data, errors):
        ceph_config = self.client.get_sync_object(Config).data
        if not ceph_config:
            return Response("Cluster configuration unavailable", status=status.HTTP_503_SERVICE_UNAVAILABLE)
        if 'pg_num' in data and data['pg_num'] > int(ceph_config['mon_max_pool_pg_num']):
            errors['pg_num'].append('requested pg_num must be <= than current limit of {max}'.format(max=ceph_config['mon_max_pool_pg_num']))

    def _check_pgp_less_than_pg_num(self, data, errors):
        if 'pgp_num' in data and 'pg_num' in data and data['pg_num'] < data['pgp_num']:
            errors['pgp_num'].append('must be >= to pg_num')

    def _check_name_unique(self, data, errors):
        if 'name' in data and data['name'] in [x.pool_name for x in [PoolDataObject(p) for p in self.client.list(POOL, {})]]:
            errors['name'].append('Pool with name {name} already exists'.format(name=data['name']))


class OsdViewSet(RPCViewSet, RequestReturner):
    """
Manage Ceph OSDs.

Apply ceph commands to an OSD by doing a POST with no data to
api/v2/cluster/<fsid>/osd/<osd_id>/command/<command>
where <command> is one of ("scrub", "deep-scrub", "repair")

e.g. Initiate a scrub on OSD 0 by POSTing {} to api/v2/cluster/<fsid>/osd/0/command/scrub

Filtering is available on this resource:

::

    # Pass a ``pool`` URL parameter set to a pool ID to filter by pool, like this:
    /api/v2/cluster/<fsid>/osd?pool=1

    # Pass a series of ``id__in[]`` parameters to specify a list of OSD IDs
    # that you wish to receive.
    /api/v2/cluster/<fsid>/osd?id__in[]=2&id__in[]=3

    """
    serializer_class = OsdSerializer

    def list(self, request):
        return self._list(request)

    def _list(self, request):
        # Get data needed for filtering
        list_filter = {}

        if 'pool' in request.GET:
            try:
                pool_id = int(request.GET['pool'])
            except ValueError:
                return Response("Pool ID must be an integer", status=status.HTTP_400_BAD_REQUEST)
            list_filter['pool'] = pool_id

        if 'id__in[]' in request.GET:
            try:
                ids = request.GET.getlist("id__in[]")
                list_filter['id__in'] = [int(i) for i in ids]
            except ValueError:
                return Response("Invalid OSD ID in list", status=status.HTTP_400_BAD_REQUEST)

        # Get data
        osds = self.client.list(OSD, list_filter)
        osd_to_pools = self.client.get_sync_object(OsdMap, ['osd_pools'])
        crush_nodes = self.client.get_sync_object(OsdMap, ['osd_tree_node_by_id'])
        osd_metadata = self.client.get_sync_object(OsdMap, ['osd_metadata'])

        osd_id_to_hostname = dict(
            [(int(osd_id), osd_meta["hostname"]) for osd_id, osd_meta in
             osd_metadata.items()])

        # Get data depending on OSD list
        osd_commands = self.client.get_valid_commands(OSD, [x['osd'] for x in osds])

        # Build OSD data objects
        for o in osds:
            # An OSD being in the OSD map does not guarantee its presence in the CRUSH
            # map, as "osd crush rm" and "osd rm" are separate operations.
            try:
                o.update({'reweight': float(crush_nodes[o['osd']]['reweight'])})
            except KeyError:
                log.warning("No CRUSH data available for OSD {0}".format(o['osd']))
                o.update({'reweight': 0.0})

            o['server'] = osd_id_to_hostname.get(o['osd'], None)

        for o in osds:
            o['pools'] = osd_to_pools[o['osd']]

        for o in osds:
            o.update(osd_commands[o['osd']])

        return Response(self.serializer_class([DataObject(o) for o in osds], many=True).data)

    def retrieve(self, request, osd_id):
        osd = self.client.get_sync_object(OsdMap, ['osds_by_id', int(osd_id)])
        crush_node = self.client.get_sync_object(OsdMap, ['osd_tree_node_by_id', int(osd_id)])
        osd['reweight'] = float(crush_node['reweight'])

        osd_metadata = self.client.get_sync_object(OsdMap, ['osd_metadata'])

        osd_id_to_hostname = dict(
            [(int(oid), osd_meta["hostname"]) for oid, osd_meta in
             osd_metadata.items()])

        osd['server'] = osd_id_to_hostname.get(osd['osd'], None)

        pools = self.client.get_sync_object(OsdMap, ['osd_pools', int(osd_id)])
        osd['pools'] = pools

        osd_commands = self.client.get_valid_commands(OSD, [int(osd_id)])
        osd.update(osd_commands[int(osd_id)])

        return Response(self.serializer_class(DataObject(osd)).data)

    def update(self, request, osd_id):
        serializer = self.serializer_class(data=request.DATA)
        if serializer.is_valid(request.method):
            return self._return_request(self.client.update(OSD, int(osd_id),
                                                           serializer.get_data()))
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def apply(self, request, osd_id, command):
        if command in self.client.get_valid_commands(OSD, [int(osd_id)]).get(int(osd_id)).get('valid_commands'):
            return Response(self.client.apply(OSD, int(osd_id), command), status=202)
        else:
            return Response('{0} not valid on {1}'.format(command, osd_id), status=403)

    def get_implemented_commands(self, request):
        return Response(OSD_IMPLEMENTED_COMMANDS)

    def get_valid_commands(self, request, osd_id=None):
        osds = []
        if osd_id is None:
            osds = self.client.get_sync_object(OsdMap, ['osds_by_id']).keys()
        else:
            osds.append(int(osd_id))

        return Response(self.client.get_valid_commands(OSD, osds))

    def validate_command(self, request, osd_id, command):
        valid_commands = self.client.get_valid_commands(OSD, [int(osd_id)]).get(int(osd_id)).get('valid_commands')

        return Response({'valid': command in valid_commands})


class OsdConfigViewSet(RPCViewSet, RequestReturner):
    """
Manage flags in the OsdMap
    """
    serializer_class = OsdConfigSerializer

    def osd_config(self, request):
        osd_map = self.client.get_sync_object(OsdMap, ['flags'])
        return Response(osd_map)

    def update(self, request):

        serializer = self.serializer_class(data=request.DATA)
        if not serializer.is_valid(request.method):
            return Response(serializer.errors, status=403)

        response = self.client.update(OSD_MAP, None, serializer.get_data())

        return self._return_request(response)


class SyncObject(RPCViewSet):
    """
These objects are the raw data received by the Calamari server from the Ceph cluster,
such as the cluster maps
    """

    def retrieve(self, request, sync_type):
        try:
            sync_type_cls = SYNC_OBJECT_STR_TYPE[sync_type]
        except KeyError:
            return Response("Unknown type '{0}'".format(sync_type), status=404)
        return Response(self.client.get_sync_object(sync_type_cls).data)

    def describe(self, request):
        return Response([s.str for s in SYNC_OBJECT_TYPES])


class ServerViewSet(RPCViewSet):
    """
Servers that we've learned about via the daemon metadata reported by
Ceph OSDs, MDSs, mons.
    """
    serializer_class = ServerSerializer

    def retrieve(self, request, fqdn):
        return Response(
            self.serializer_class(
                DataObject(self.client.server_get(fqdn))).data
        )

    def list(self, request):
        servers = self.client.server_list()
        return Response(self.serializer_class(
            [DataObject(s) for s in servers],
            many=True).data)


class MonViewSet(RPCViewSet):
    """
Ceph monitor services.

Note that the ID used to retrieve a specific mon using this API resource is
the monitor *name* as opposed to the monitor *rank*.

The quorum status reported here is based on the last mon status reported by
the Ceph cluster, and also the status of each mon daemon queried by Calamari.

For debugging mons which are failing to join the cluster, it may be
useful to show users data from the /status sub-url, which returns the
"mon_status" output from the daemon.

    """
    serializer_class = MonSerializer

    def _get_mons(self):
        monmap_mons = self.client.get_sync_object(MonMap).data['mons']
        mon_status = self.client.get_sync_object(MonStatus).data

        for mon in monmap_mons:
            mon['in_quorum'] = mon['rank'] in mon_status['quorum']
            mon['server'] = self.client.get_metadata("mon", mon['name'])['hostname']
            mon['leader'] = mon['rank'] == mon_status['quorum'][0]

        return monmap_mons

    def retrieve(self, request, mon_id):
        mons = self._get_mons()
        try:
            mon = [m for m in mons if m['name'] == mon_id][0]
        except IndexError:
            raise Http404("Mon '%s' not found" % mon_id)

        return Response(self.serializer_class(DataObject(mon)).data)

    def list(self, request):
        mons = self._get_mons()
        return Response(
            self.serializer_class([DataObject(m) for m in mons],
                                  many=True).data)

