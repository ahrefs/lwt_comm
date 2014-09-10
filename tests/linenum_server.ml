open Lwt
module C = Lwt_comm

let (linenum_server, _server_ctl) = C.duplex begin fun conn ->
  let rec loop n =
    match_lwt C.recv_opt conn with
    | None -> C.close conn; return_unit
    | Some line ->
        let out = string_of_int n ^ ": " ^ line in
        lwt () = C.send conn out in
        loop (n + 1)
  in
    loop 1
end

let ch_of_fd fd =
  (Lwt_io.of_fd ~mode:Lwt_io.input fd, Lwt_io.of_fd ~mode:Lwt_io.output fd)

let linenum_unix_serverfunc conn fd =
  let (inch, outch) = ch_of_fd fd in
  let rec loop () =
    let line_opt =
      try_lwt
        lwt line = Lwt_io.read_line inch in
        (* Printf.eprintf ">%s<\n%!" line; *)
        return @@ Some line
      with
        End_of_file ->
          lwt () = Lwt_io.flush outch in
          return_none
    in
    match_lwt line_opt with
    | None -> return_unit
    | Some line ->
        lwt () = C.send conn line in
        lwt out = C.recv conn in
        lwt () = Lwt_io.write_line outch out in
        loop ()
  in
    loop ()

let linenum_unix_serverfunc2 : (_, _, [`Connect | `Bidi]) C.unix_func =
  C.unix_func_of_maps
    Lwt_io.read_line
    Lwt_io.write_line

let () = Lwt_main.run begin
  try_lwt
    let open Lwt_unix in
    C.run_unix_server
      linenum_server
      PF_INET
      SOCK_STREAM
      0
      (ADDR_INET (Unix.inet_addr_loopback, 34567))
      linenum_unix_serverfunc;
    Lwt_unix.sleep 1000.
  with
    e -> Printf.eprintf "%s: %s\n%!" Sys.argv.(0) (Printexc.to_string e);
      return_unit
end
