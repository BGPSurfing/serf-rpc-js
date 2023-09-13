import * as net from "node:net";
import * as msgpack from "msgpack-lite";
import { EventEmitter } from "node:events";

const NOOP: number = 0;

export enum SerfCmd {
  handshake = "handshake",
  auth = "auth",
  event = "event",
  force_leave = "force-leave",
  join = "join",
  members = "members",
  members_filtered = "members-filtered",
  tags = "tags",
  stream = "stream",
  monitor = "monitor",
  stop = "stop",
  leave = "leave",
  query = "query",
  respond = "respond",
  install_key = "install-key",
  use_key = "use-key",
  remove_key = "remove-key",
  list_keys = "list-keys",
  stats = "stats",
  get_coordinate = "get-coordinate",
}

export enum SerfErr {
  Invalid, // todo
}

interface CmdBase {
  Seq: number;
}

interface CmdHead extends CmdBase {
  Command: string;
}

interface CmdResp extends CmdBase {
  Error: string;
}

interface CmdOpt {
  stream: boolean; // expected data after subbed
  request: boolean; // requires body after header
  response: boolean; // responds data after header
}

interface CmdState {
  stream: boolean; // for event
  response: boolean; // for command

  reject: Function;
  resolve: Function;
}

const CmdList: { [Cmd in SerfCmd]: CmdOpt } = {
  [SerfCmd.handshake]: {
    stream: false,
    request: true,
    response: false,
  },
  [SerfCmd.auth]: {
    stream: false,
    request: true,
    response: false,
  },
  [SerfCmd.event]: {
    stream: false,
    request: true,
    response: false,
  },
  [SerfCmd.force_leave]: {
    stream: false,
    request: true,
    response: false,
  },
  [SerfCmd.join]: {
    stream: false,
    request: true,
    response: true,
  },
  [SerfCmd.members]: {
    stream: false,
    request: false,
    response: true,
  },
  [SerfCmd.members_filtered]: {
    stream: false,
    request: true,
    response: true,
  },
  [SerfCmd.tags]: {
    stream: false,
    request: true,
    response: false,
  },
  [SerfCmd.stream]: {
    stream: true,
    request: false,
    response: false,
  },
  [SerfCmd.monitor]: {
    stream: true,
    request: true,
    response: false,
  },
  [SerfCmd.stop]: {
    stream: false,
    request: true,
    response: false,
  },
  [SerfCmd.leave]: {
    stream: false,
    request: false,
    response: false,
  },
  [SerfCmd.query]: {
    stream: true,
    request: true,
    response: true,
  },
  [SerfCmd.respond]: {
    stream: false,
    request: true,
    response: false,
  },
  [SerfCmd.install_key]: {
    stream: false,
    request: true,
    response: true,
  },
  [SerfCmd.use_key]: {
    stream: false,
    request: true,
    response: true,
  },
  [SerfCmd.remove_key]: {
    stream: false,
    request: true,
    response: true,
  },
  [SerfCmd.list_keys]: {
    stream: false,
    request: false,
    response: true,
  },
  [SerfCmd.stats]: {
    stream: false,
    request: false,
    response: true,
  },
  [SerfCmd.get_coordinate]: {
    stream: false,
    request: true,
    response: true,
  },
};

export class SerfClient extends EventEmitter {
  private host: string;
  private port: number;

  private seq: number;
  private next: number;

  private alive: boolean;

  private socket: net.Socket;
  private stream: msgpack.DecodeStream;

  private handlers: { [seq: number]: CmdState } = {};

  constructor(host: string, port: number) {
    super();

    this.host = host;
    this.port = port;

    this.seq = NOOP;
    this.next = NOOP;

    this.alive = false;

    this.socket = new net.Socket();
    this.socket.setNoDelay(true);

    this.stream = msgpack.createDecodeStream();
    this.stream.on("data", (data) => this.handle(data));

    this.socket.pipe(this.stream);
  }

  private getHandler(seq: number): CmdState | null {
    return seq in this.handlers ? this.handlers[seq] : null;
  }
  private remHandler(seq: number): void {
    delete this.handlers[seq];
  }

  private handle(data: any): void {
    if (this.alive) {
      if ("Seq" in data) {
        this.handleResp(data);
      } else {
        this.handleBody(data);
      }
    }
  }

  private handleResp(resp: CmdResp): void {
    const seq = resp.Seq;
    const state = this.getHandler(seq);

    if (state === null) {
      this.emit("error", SerfErr.Invalid, seq);
      return;
    }

    const error = resp.Error;

    if (error.length) {
      this.remHandler(seq);
      state.reject(new Error(error));
    } else if (state.response) {
      this.next = seq;
      if (state.stream) {
        state.response = false;
        state.resolve(seq);
      }
    } else if (state.stream) {
      this.next = seq;
    } else {
      this.remHandler(seq);
      state.resolve(seq);
    }
  }
  private handleBody(data: any) {
    const seq = this.next;
    const state = this.getHandler(seq);

    this.next = NOOP;

    if (state === null) {
      this.emit("error", seq, SerfErr.Invalid);
      return;
    }

    if (state.stream) {
      this.emit("stream", seq, data);
      return;
    }

    this.remHandler(seq);

    if (state.response) {
      state.resolve(data);
    } else {
      state.reject(new Error("Invalid Responsee"));
    }
  }

  private write(head: CmdHead, body: any = null) {
    const headData = msgpack.encode(head);
    this.socket.write(headData);

    if (body) {
      const bodyData = msgpack.encode(body);
      this.socket.write(bodyData);
    }
  }

  public async request(cmd: SerfCmd, body: any = null): Promise<any> {
    return new Promise<any>((resolve, reject) => {
      if (!this.alive) {
        reject(new Error("Client is not alive"));
      }

      const seq = ++this.seq;

      const { stream, request, response } = CmdList[cmd];

      if (request && body === null) {
        reject(`${cmd} requires a request body`);
      }

      this.handlers[seq] = {
        stream: stream,
        response: response,

        reject: (err: any) => reject(err),
        resolve: (data: any) => resolve(data),
      };

      let handled = false;

      this.socket.prependOnceListener("error", (err) => {
        if (handled === false) {
          this.remHandler(seq);
          reject(err);
        }
      });

      this.write({ Seq: seq, Command: cmd }, body);

      handled = true;

      if (stream && response === false) {
        resolve(seq);
      }
    });
  }

  public async handshake(): Promise<any> {
    return this.request(SerfCmd.handshake, { Version: 1 });
  }

  public async open(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      let complete = false;

      this.socket.once("error", (error: Error) => {
        if (complete) {
          return;
        }

        complete = true;
        this.alive = false;

        reject(error);
      });

      this.socket.connect(this.port, this.host, () => {
        if (complete) {
          return;
        }

        this.removeAllListeners("error");

        complete = true;
        this.alive = true;

        resolve();
      });
    });
  }
  public async close(): Promise<void> {
    return new Promise<void>((resolve) =>
      this.socket.end(() => {
        this.alive = false;
        resolve();
      })
    );
  }
}
