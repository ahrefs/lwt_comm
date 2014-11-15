(** Program to show how forgotten confirmations are handled. *)

open Lwt

module C = Lwt_comm

let (server, _ctl) = C.duplex @@ fun conn ->
  lwt () = C.recv conn in
  fail @@ Exit

let () = Lwt_main.run begin
  let c = C.connect server in
  lwt () = C.send c () in
  assert false
end
