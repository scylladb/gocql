=====================================================
Client routes (PrivateLink / Private Service Connect)
=====================================================

Client routes let the driver connect to a ScyllaDB Cloud cluster through private
endpoints instead of the public host addresses. ScyllaDB Cloud exposes a
``system.client_routes`` table that maps each host to the private endpoint that
serves it; when client routes are enabled, the driver reads that table and
translates every host address to its per-host private endpoint address and port.

Client routes require server support for both ``system.client_routes`` and the
``CLIENT_ROUTES_CHANGE`` event.

This feature is also known as **PrivateLink** support, **private link**,
**private service connection**, AWS **PrivateLink** (**PL**), and GCP
**Private Service Connect** (**PSC**). The driver API is named after the
``system.client_routes`` table, so it is also written as **client_routes** or
**clientroutes**.

Limitations
===========

**Mixed clusters are not supported.** Every node must be reachable through client
routes, that is, have a row in ``system.client_routes`` for one of the connection
IDs you configured. If a node has no matching route, the driver fails to translate
its address and does not fall back to the node's broadcast address — connections to
that node fail.

Enabling client routes
======================

Use ``WithClientRoutes`` and pass the connection IDs you receive from
ScyllaDB Cloud:

.. code-block:: go

   cluster := gocql.NewCluster("private-link.dns.name")
   cluster.WithOptions(
       gocql.WithClientRoutes(
           gocql.WithEndpoints(
               gocql.ClientRoutesEndpoint{ConnectionID: "your-connection-id"},
           ),
       ),
   )

At least one connection ID is required. Configuring client routes without any
endpoint is a configuration error and session creation fails. Every configured
endpoint must have a non-empty ``ConnectionID``.

The driver loads route rows and accepts route-change events only for configured
connection IDs.

Note that the contact points themselves are **not** translated. Translation is
keyed by host ID, and the initial contact point has no host ID yet, so it is dialed
exactly as configured. The address you pass to ``NewCluster`` must therefore already
be reachable from the client — normally a private endpoint address. Only the cluster
nodes the driver subsequently discovers are routed through ``system.client_routes``.

Multiple connection IDs
=======================

If your deployment routes traffic through more than one private endpoint — for
example one per availability zone — configure all of them. ``WithEndpoints`` is
variadic, and each row of ``system.client_routes`` is matched against the whole
list:

.. code-block:: go

   cluster := gocql.NewCluster()
   cluster.WithOptions(
       gocql.WithClientRoutes(
           gocql.WithEndpoints(
               gocql.ClientRoutesEndpoint{
                   ConnectionID:   "conn-id-az-1",
                   ConnectionAddr: "endpoint-az-1.eu-west-1.vpce.amazonaws.com",
               },
               gocql.ClientRoutesEndpoint{
                   ConnectionID:   "conn-id-az-2",
                   ConnectionAddr: "endpoint-az-2.eu-west-1.vpce.amazonaws.com",
               },
           ),
       ),
   )

When several configured endpoints provide a route to the same host, the driver
picks one preferred route per host and keeps using it until that route disappears.
A connection-health failure alone does not switch routes. If a
``CLIENT_ROUTES_CHANGE`` update removes the selected route, the next translation
selects another configured route for that host.

Overriding the endpoint address
===============================

``ClientRoutesEndpoint`` has two fields:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Field
     - Description
   * - ``ConnectionID``
     - Required. The ScyllaDB Cloud connection ID to read from
       ``system.client_routes``.
   * - ``ConnectionAddr``
     - Optional. IP address or DNS name of the private endpoint. When empty, the
       driver uses the address from the ``system.client_routes`` table. When set, it
       **overrides** that address for every route belonging to this connection ID —
       useful when your environment needs a different DNS name or IP, for example in
       local testing. Supply an address without a port; the port always comes from
       the table for discovered hosts.

``ConnectionAddr`` has a second effect: if the cluster has no hosts configured when
``WithClientRoutes`` is applied, every non-empty ``ConnectionAddr`` is also used as
an initial contact point. This lets you seed the cluster entirely from private
endpoint hostnames:

.. code-block:: go

   cluster := gocql.NewCluster() // no public contact points
   cluster.WithOptions(
       gocql.WithClientRoutes(
           gocql.WithEndpoints(
               gocql.ClientRoutesEndpoint{
                   ConnectionID:   "your-connection-id",
                   ConnectionAddr: "vpce-0123456789abcdef.vpce-svc-0123456789abcdef.eu-west-1.vpce.amazonaws.com",
               },
           ),
       ),
   )

These inferred contact points use ``ClusterConfig.Port``, which defaults to
9042. Specify contact points explicitly with ``NewCluster`` when bootstrap
endpoints use different ports. An embedded port applies only while dialing that
contact point; connections to discovered hosts use ``port`` or ``tls_port`` from
the route table.

Shard awareness
===============

Advanced shard awareness — where the driver picks a connection's *source* port so
that ScyllaDB assigns the connection to a specific shard — is **disabled by
default** when client routes are enabled, because private endpoint paths commonly
use NAT that rewrites source ports.

Basic shard awareness is unaffected: the driver still maintains per-shard
connection pools and routes each request to the connection for the right shard.
Each connection's shard is learned from the server during the handshake, so pooling
and token-to-shard routing do not depend on the shard-aware port.

Without the shard-aware port, though, the driver cannot ask for a specific shard,
so it fills the pool by opening connections until every shard is covered. A
connection landing on an already-covered shard is **not** closed straight away: it
is kept in a bounded excess pool, and the whole pool is closed in one batch once
either every shard has a connection or the pool grows past
``MaxExcessShardConnectionsRate`` times the host's shard count (the rate defaults
to ``2``). While a pool fills, expect open connections to a host to overshoot the
shard count — with the default rate, up to roughly three times it.

Excess connections are never used to serve requests, and in the current
implementation they are not promoted into the pool when a live connection drops;
they are only held and later closed.

Enable advanced shard awareness only when both of the following hold:

* the private endpoint forwards to ScyllaDB's Proxy Protocol v2 shard-aware CQL
  listener (``native_shard_aware_transport_port_proxy_protocol``, or
  ``native_shard_aware_transport_port_ssl_proxy_protocol`` for TLS), and
* the proxy supplies the original client source port in the Proxy Protocol v2
  header.

Both listeners are ScyllaDB server-side configuration parameters; see
`native_shard_aware_transport_port_proxy_protocol
<https://docs.scylladb.com/manual/stable/reference/configuration-parameters.html#confprop-native-shard-aware-transport-port-proxy-protocol>`_ and
`native_shard_aware_transport_port_ssl_proxy_protocol
<https://docs.scylladb.com/manual/stable/reference/configuration-parameters.html#confprop-native-shard-aware-transport-port-ssl-proxy-protocol>`_ in the ScyllaDB
configuration parameters reference.

The default and ``WithShardAwareness`` option were added after ``v1.19.0``. Use
a later release or a pseudo-version containing commit ``e36b80d0``. With
``v1.19.0``, client routes leave advanced shard awareness enabled by default;
set ``ClusterConfig.DisableShardAwarePort = true`` when the endpoint does not
preserve source ports.

.. code-block:: go

   cluster.WithOptions(
       gocql.WithClientRoutes(
           gocql.WithEndpoints(
               gocql.ClientRoutesEndpoint{ConnectionID: "your-connection-id"},
           ),
           gocql.WithShardAwareness(true),
       ),
   )

``ClusterConfig.DisableShardAwarePort`` takes precedence: if it is set to
``true``, ``WithShardAwareness(true)`` has no effect.

TLS
===

TLS is supported. ``system.client_routes`` exposes both a plain ``port`` and a
``tls_port`` for each route; the driver selects both columns in its query and uses
the ``tls_port`` whenever ``ClusterConfig.SslOpts`` is set. Every selected TLS
route must provide ``tls_port``. No client-routes-specific TLS configuration is
needed.

Custom ``HostDialer`` implementations are not supported with client routes.
``HostInfo`` exposes the translated route address but not its translated port,
so a custom host dialer may use the node-advertised port instead. Use
``ClusterConfig.Dialer`` when client routes require custom dialing.

Configuration options
=====================

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Option
     - Description
   * - ``WithEndpoints(endpoints ...ClientRoutesEndpoint)``
     - Sets the private endpoints to use. At least one is required.
   * - ``WithTable(tableName string)``
     - Overrides the route table for tests. Production use requires
       ``system.client_routes``; do not set this option in production.
   * - ``WithShardAwareness(enabled bool)``
     - Opts in to advanced shard awareness. Disabled by default. See
       `Shard awareness`_.

Deprecated options
------------------

The following options no longer have any effect and will be removed in a future
release: ``WithMaxResolverConcurrency``, ``WithResolveHealthyEndpointPeriod``, and
the ``ClientRoutesConfig`` fields ``ResolveHealthyEndpointPeriod``,
``ResolverCacheDuration``, ``MaxResolverConcurrency``, and
``BlockUnknownEndpoints``. Unknown endpoints are always blocked.

How routes are kept up to date
==============================

The driver reads ``system.client_routes`` once when the session starts, and then
refreshes it in response to events rather than by polling:

* a ``CLIENT_ROUTES_CHANGE`` event is filtered down to the connection IDs you
  configured, and ignored if none of them remain. If the event also names host IDs,
  only those (connection ID, host ID) pairs are re-read; if it names none, every
  host on the affected connection IDs is re-read;
* if the control connection is recreated, all configured connection IDs are
  re-read.

The driver only registers for ``CLIENT_ROUTES_CHANGE`` when client routes are
configured.

Interaction with AddressTranslator
==================================

Client routes are implemented as an address translator. ``ClientRoutesConfig`` and
``ClusterConfig.AddressTranslator`` are therefore mutually exclusive — setting both
fails validation with ``AddressTranslator and ClientRoutesConfig should not be set
at the same time``.
