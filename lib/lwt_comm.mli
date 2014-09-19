(** Type that represents in-process server.  It receives messages with type
    ['req] and sends messages with type ['resp].
    Clients send messages with type ['req] and receive messages with type
    ['resp].
    Type parameters names don't mean that every request must have only one
    response: communication is bidirectional/duplex.  Think it's a typed
    TCP/IP.
    ['kind] is almost not used now, it's left for future api extensions to
    support other kinds of servers: servers with strict request-response
    style of work, servers that don't require connection (stateless).
 *)
type (-'req, +'resp, 'kind) server

(** Type of value used to control server: for now, it's possible to shutdown
    server and to wait for server shutdown using value of this type.
    This functionality is separated from values of type [server], since there
    may exist code that should be able to only connect to server, but not to
    shutdown it, and this privileges can be separated by hiding [server_ctl]
    value from code that shouldn't be able to control server.
 *)
type server_ctl

(** Type that represents connection to in-process server or client.
    Values of type ['snd] are sent to connection, values of type ['rcv] are
    received.
 *)
type (-'snd, +'rcv, 'kind) conn

(** *)
type -'snd confirmation

val send : ('snd, 'rcv, 'kind) conn -> 'snd -> unit Lwt.t

val recv : ('snd, 'rcv, 'kind) conn -> 'rcv Lwt.t

val recv_ack : ('snd, 'rcv, 'kind) conn -> ('rcv * 'rcv confirmation) Lwt.t

val ack : 'rcv confirmation -> unit

val nack : 'rcv confirmation -> exn -> unit

(** Same as [recv], but maps error [End_of_file] to [None], and wraps received
    values in [Some]. *)
val recv_opt : ('snd, 'rcv, 'kind) conn -> 'rcv option Lwt.t

val recv_res :
  ('snd, 'rcv, 'kind) conn -> [ `Ok of 'rcv | `Error of exn ] Lwt.t

val recv_res_ack :
  ('snd, 'rcv, 'kind) conn ->
  [ `Ok of ('rcv * 'rcv confirmation) | `Error of exn ] Lwt.t

val shutdown :
  ?exn:exn ->
  ('snd, 'rcv, 'kind) conn ->
  Unix.shutdown_command ->
  unit

val close : ?exn:exn -> ('snd, 'rcv, 'kind) conn -> unit

(** [duplex serverfunc] creates a server that work in duplex mode, without
    special request-response interaction patterns.
    [serverfunc] is called with connection from client passed as argument,
    when client connects to server.
    Each connection spawns an lwt thread returned by [serverfunc].
    When [serverfunc] returns [()], connection is closed automatically,
    if it was not closed before.
 *)
val duplex :
  (('resp, 'req, [`Bidi | `Connect] as 'k) conn -> unit Lwt.t) ->
  (('req, 'resp, 'k) server * server_ctl)

val connect :
  ?ack_req:bool -> ?ack_resp:bool ->
  ('req, 'resp, [> `Connect] as 'k) server ->
  ('req, 'resp, 'k) conn

(** with-idiom for [connect]. *)
val with_connection :
  ('req, 'resp, [> `Connect] as 'k) server ->
  (('req, 'resp, 'k) conn -> 'a Lwt.t) -> 'a Lwt.t

exception Server_shut_down

(** Shutdown server: future [connect]s will raise [?exn] (default exception is
    [Server_shut_down].
    Server's on_shutdown is executed when server is shut down.
 *)
val shutdown_server : ?exn:exn -> server_ctl -> unit Lwt.t

(** Wait for server's shutdown without trying to shut down server.
    When [wait_for_server_shutdown] returns, server's on_shutdown is already
    executed.
 *)
val wait_for_server_shutdown : server_ctl -> unit Lwt.t

(** Create server that has requests/responses mapped by given functions. *)
val map_server :
  ('req2 -> 'req1) ->
  ('resp1 -> 'resp2) ->
  ('req1, 'resp1, 'k) server ->
  ('req2, 'resp2, 'k) server

(** Create connection that has requests/responses mapped by given functions. *)
val map_conn :
  ('req2 -> 'req1) ->
  ('resp1 -> 'resp2) ->
  ('req1, 'resp1, 'k) conn ->
  ('req2, 'resp2, 'k) conn

(** Type of layer between [server] and tcp/ip socket.
    This function takes [conn] and file descriptor, and passes messages back
    and forth.  When function exits, both connections are closed.
 *)
type (+'req, -'resp, 'k) unix_func =
  ('req, 'resp, 'k) conn -> Lwt_unix.file_descr -> unit Lwt.t

(** Runs server that listens on given address, on incoming network connection
    this function connects to [server] and runs an lwt thread
    [unix_func conn fd] that parses bytes from socket and sends typed requests
    to [server], and outputs [server] responses to socket.
    Unhandled exceptions are currently dumped to stderr.
    Listening for connections stops when [server] is shut down.
 *)
val run_unix_server :
  ('req, 'resp, [> `Bidi | `Connect] as 'k) server ->
  Lwt_unix.socket_domain -> Lwt_unix.socket_type -> int (* proto; 0 *) ->
  Lwt_unix.sockaddr ->
  ?listen:int ->
  ('req, 'resp, 'k) unix_func ->
  unit

(** Creates [unix_func] for simple cases: when protocol on sockets maps 1:1
    to protocol on [server].
    [?setup_fd] is called before creating Lwt_io channels, used to set up
    socket-specific options.
    Unhandled exceptions are currently dumped to stderr.
 *)
val unix_func_of_maps :
  ?setup_fd : (Lwt_unix.file_descr -> unit Lwt.t) ->
  ?on_server_close:
    (Lwt_io.input_channel -> Lwt_io.output_channel -> exn -> unit Lwt.t) ->
  (Lwt_io.input_channel -> 'req Lwt.t) ->
  (Lwt_io.output_channel -> 'resp -> unit Lwt.t) ->
  ('req, 'resp, 'k) unix_func

val connect_unix :
  ('resp, 'req, [> `Bidi | `Connect ] as 'k) unix_func ->
  Lwt_unix.socket_domain -> Lwt_unix.socket_type -> int (* proto; 0 *) ->
  Lwt_unix.sockaddr ->
  ('req, 'resp, 'k) conn Lwt.t
