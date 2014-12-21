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
    shutdown it, and these privileges can be separated by hiding [server_ctl]
    value from code that shouldn't be able to control server.
 *)
type server_ctl

(** Type that represents connection to in-process server or client.
    Values of type ['snd] are sent to connection, values of type ['rcv] are
    received.
 *)
type (-'snd, +'rcv, 'kind) conn

(** ACKs.

    Connection part (sending or receiving) can behave as
    - message queue in memory, sender puts values, receiver takes them.
      No ACKs or other flow control.
    - single memory cell that requires receiver to confirm processing of
      message.  This is needed in cases when message processing can fail due
      to real world issues, for example, when server sends/receives data from
      socket.  Here the error must be raised as soon as possible: [send] call
      must fail.  We say that such connection "uses ACKs".

    [connect]'s [?ack_req] and [?ack_resp] arguments determine whether sending
    and receiving parts of connection to server will use ACKs.

    When client/server has processed message received using [recv_no_ack]
    ([recv_no_ack_opt], [recv_no_ack_res]), [ack] function must be called
    on this connection.  For ACK-enabled connection, [ack]:
    - tells sender "the message was processed ok, now wake up and continue your
      tasks"
    - tells other senders (that are blocked waiting for their chance to send
      request to server) "now next sender can wake up and send message"

    When there is an error processing message, connection must be closed using
    [close ~exn:..] (in case of server, if error is an exception, it can be
    thrown from connection's handler thread, connection will be closed too).

    When connection is closed with exception, all waiting senders wake up with
    given exception.
 *)

(** Send message to connection.
    Raises exception when connection is closed.
 *)
val send : ('snd, 'rcv, 'kind) conn -> 'snd -> unit Lwt.t

val send_res : ('snd, 'rcv, 'kind) conn -> 'snd ->
               [ `Ok of unit | `Error of exn ] Lwt.t

(** Receive message from connection and don't send ACK.
    Raises exception when connection is closed.
    When message is processed, call [ack conn] on success or close connection
    on failure.
 *)
val recv_no_ack : ('snd, 'rcv, 'kind) conn -> 'rcv Lwt.t

(** Same as [recv_no_ack], but maps error [End_of_file] to [None], and wraps
    received values in [Some]. *)
val recv_no_ack_opt : ('snd, 'rcv, 'kind) conn -> 'rcv option Lwt.t

val recv_no_ack_res :
  ('snd, 'rcv, 'kind) conn -> [ `Ok of 'rcv | `Error of exn ] Lwt.t

(** Mark last message from this connection as received and processed.
    No-op for connections which receiving part doesn't use ACKs.
    Note: call [ack] only once for every processed message, otherwise it will
    raise error.
 *)
val ack : ('snd, 'rcv, 'kind) conn -> unit

(** = [recv_no_ack] + [ack].  Should be used when ACKs are not needed. *)
val recv : ('snd, 'rcv, 'kind) conn -> 'rcv Lwt.t

val recv_opt : ('snd, 'rcv, 'kind) conn -> 'rcv option Lwt.t

val recv_res :
  ('snd, 'rcv, 'kind) conn -> [ `Ok of 'rcv | `Error of exn ] Lwt.t

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
  ?on_shutdown:(unit -> unit Lwt.t) ->
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
    [Server_shut_down], and current connections are closed with same exception.
    It's expected that server and client know how to handle given exception
    when they receive it while reading from connections.
 *)
val shutdown_server : ?exn:exn -> server_ctl -> unit Lwt.t

(** Constructors of ['a wait] are used in [shutdown_server_wait]. *)
type _ wait =
  | Time : float -> [ `Instances_exist of int | `Shut_down ] wait
  | Forever : unit wait
  | Don't : int wait

(** Make future [connect]s to server fail with [?exn], and wait for current
    connection handler instances to exit.
    [?wait] can be
    - [Time s], where [s] is the number of seconds to wait, it must be not
      negative.  Returned value can be:
      - [`Instances_exist n] -- [n] instances still connected to server
      - [ `Shut_down ] -- server has shut down
    - [Forever] -- wait for server shutdown forever, [()] is returned.
    - [Don't] -- don't wait for server shutdown.  Returned value is number of
      instances connected to server now.
    Default is "wait forever".
  *)
val shutdown_server_wait : ?exn:exn -> server_ctl -> 'a wait -> 'a Lwt.t

(** Same as [shutdown_server_wait], but waits infinite time and returns unit. *)
val shutdown_server_wait_infinite : ?exn:exn -> server_ctl -> unit Lwt.t

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
    Argument with type [Lwt_io.input_channel -> 'req Lwt.t] can throw
    [End_of_file] to indicate normal connection closing.
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

(** Create new server ("outer server") that automates reconnections to server
    given as argument ("inner server").  This is useful only when inner server
    doesn't initiate connection closure.
    Also protocol must support reconnects (for example, it must not use
    specific connection procedure).
    For every connection to outer server there exists one connection to inner
    server.
    When connection to outer server is open, and inner server closes
    connection, reconnect happens, and outer server resends last non-ACKed
    message to new connection (it means that server must respect ACKs,
    otherwise there will be nothing to resend).
    First argument is used to create per-connection stream with reconnection
    timeouts in seconds.  End of stream means "stop reconnecting".  For
    example, when reconnection must be immediate and non-stop, one can use
    [Stream.from (fun () -> Some 0.)] as an argument.
 *)
val reconnecting_server :
  (unit -> float Stream.t) ->
  (('req, 'resp, [> `Connect | `Bidi ]) server * server_ctl) ->
  (('req, 'resp, [> `Connect | `Bidi ]) server * server_ctl)

(** Switch is a server ("outer server") that allows one to communicate with
    different instances of servers ("inner servers") depending on "key" (part
    of request).  It's like a "pool", but instead of spawning some number of
    equal server instances it opens one connection per key.  There must be
    specified a way to get key from request sent to outer server.  Keys must be
    ordered with [?key_compare] optional argument (default is
    [Pervasives.compare]).

             /-switch-server (outer)-\            /---connection-for---\
             |                       |            |-this-request's-key-|
             |       split:          |            |                    |
     'orq -> | 'orq -> ('key * 'irq) | -> 'irq -> |    -->-->-->-\     |
             |                       |            |              |     |
             |      combine¹:        |            |              |     |
     'ors <- | 'key -> 'irs -> 'ors  | <- 'irs <- |    --<--<--<-/     |
             |                       |            |                    |
             \-----------------------/            \--------------------/

     ¹ -- actual type is more complex and gives user more power.

    Functional argument with type ['key -> ('irq, 'irs, [> `Bidi ]) conn] can,
    for example,
    - spawn servers
    - remember and reuse connections to servers for every ['key] and return it
    - connect to some server, tell it "this connection will work with this
      specific key" and return this "initialized" connection
 *)
val switch :
  ?key_compare:('key -> 'key -> int) ->
  split:('orq -> ('key * 'irq)) ->
  combine:('key ->
           [ `Ok of 'irs | `Error of exn ] ->
           [ `Ok of 'ors | `Error of exn ]) ->
  ('key -> ('irq, 'irs, [> `Bidi ]) conn) ->
  ('orq, 'ors, [> `Connect | `Bidi ]) server * server_ctl

(** Connection names can be used for debug purposes.  Note: when connection's
    name is registered, there is no way to unregister it and to free memory,
    so use this in short tests only. *)
val set_conn_name : ('a, 'b, 'c) conn -> string -> unit
val conn_name : ('a, 'b, 'c) conn -> string
