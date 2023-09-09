import * as net from "node:net";
import * as msgpack from "msgpack-lite";

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

interface SerfOpt {
  request: boolean;
  response: boolean;
}

const SerfCommands: { [Cmd in SerfCmd]: SerfOpt } = {
  [SerfCmd.handshake]: {
    request: true,
    response: false,
  },
  [SerfCmd.auth]: {
    request: true,
    response: false,
  },
  [SerfCmd.event]: {
    request: true,
    response: false,
  },
  [SerfCmd.force_leave]: {
    request: true,
    response: false,
  },
  [SerfCmd.join]: {
    request: true,
    response: true,
  },
  [SerfCmd.members]: {
    request: false,
    response: true,
  },
  [SerfCmd.members_filtered]: {
    request: true,
    response: true,
  },
  [SerfCmd.tags]: {
    request: true,
    response: false,
  },
  [SerfCmd.stream]: {
    // No Support
    request: false,
    response: false,
  },
  [SerfCmd.monitor]: {
    // No Support
    request: true,
    response: false,
  },
  [SerfCmd.stop]: {
    request: true,
    response: false,
  },
  [SerfCmd.leave]: {
    request: false,
    response: false,
  },
  [SerfCmd.query]: {
    // No Support
    request: true,
    response: true,
  },
  [SerfCmd.respond]: {
    request: true,
    response: false,
  },
  [SerfCmd.install_key]: {
    request: true,
    response: true,
  },
  [SerfCmd.use_key]: {
    request: true,
    response: true,
  },
  [SerfCmd.remove_key]: {
    request: true,
    response: true,
  },
  [SerfCmd.list_keys]: {
    request: false,
    response: true,
  },
  [SerfCmd.stats]: {
    request: false,
    response: true,
  },
  [SerfCmd.get_coordinate]: {
    request: true,
    response: true,
  },
};

interface Callback {
  reject: Function;
  resolve: Function;
}

interface CallbackEx extends Callback {
  response: boolean;
}

export class SerfClient {
  private host: string;
  private port: number;

  private seq: number;
  private next: number;

  private alive: boolean;

  private socket: net.Socket;
  private stream: msgpack.DecodeStream;

  private handlers: { [key: string]: Callback } = {};
  private requests: { [key: number]: CallbackEx } = {};

  constructor(host: string, port: number) {
    this.host = host;
    this.port = port;

    this.seq = 1;
    this.next = 0;

    this.alive = false;

    this.socket = new net.Socket();
    this.socket.setNoDelay(true);

    this.stream = msgpack.createDecodeStream();
    this.stream.on("data", (data) => this.handle(data));

    this.socket.pipe(this.stream);
  }

  private handle(data: any): void {
    if (this.alive) {
      if (this.next) {
        this.handleEnd(data);
      } else if ("Seq" in data) {
        this.handleSeq(data);
      } else if ("event" in data) {
        this.handleEvt(data);
      }
    }
  }

  private handleEvt(data: any) {
    const event = data["event"];

    if (this.handlers[event]) {
      const cb = this.handlers[event];
      cb.resolve(data);
    } else {
      console.warn(`Unhandled event type: ${event}`);
    }
  }

  private handleSeq(data: any) {
    const seq = data["Seq"];
    const error = data["Error"];

    if (this.requests[seq]) {
      const cb = this.requests[seq];

      if (error.length) {
        cb.reject(new Error(error));
      }

      if (cb.response) {
        this.next = seq;
      } else {
        delete this.requests[seq];
        cb.resolve(seq);
      }
    } else if (error.length) {
      console.warn(`Unhandled Seq err: ${error}`);
    } else {
      console.warn(`Unhandled Seq num: ${seq}`);
    }
  }

  private handleEnd(data: any) {
    const seq = this.next;

    if (this.requests[seq]) {
      const cb = this.requests[seq];

      delete this.requests[seq];

      this.next = 0;

      if (cb.response) {
        cb.resolve(data);
      } else {
        cb.reject("Invalid Seqeuence");
      }
    } else {
      console.warn(`Unhandled Seq num: ${seq}`);
      console.warn(`Unhandled Seq body: ${data}`);
    }
  }

  public event(eventType: string, handler: Function): SerfClient {
    this.handlers[eventType] = {
      reject: (err: any) => handler(err),
      resolve: (data: any) => handler(data),
    };

    return this;
  }

  public async open(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.socket.once("error", (err) => reject(err));

      this.socket.connect(this.port, this.host, () => {
        this.alive = true;
        resolve();
      });
    });
  }

  public async send(cmd: SerfCmd, body: any = null): Promise<any> {
    return new Promise<any>((resolve, reject) => {
      if (!this.alive) {
        reject(new Error("Client is not alive"));
      }

      const Seq = this.seq++;

      const { request, response } = SerfCommands[cmd];

      if (request && !body) {
        reject(`${cmd} requires a body`);
      }

      this.requests[Seq] = {
        reject: (err: any) => reject(err),
        resolve: (data: any) => resolve(data),
        response: response,
      };

      this.socket.once("error", (err) => {
        delete this.requests[Seq];
        reject(err);
      });

      const header = msgpack.encode({ Command: cmd, Seq });
      this.socket.write(header);

      if (body) {
        const payload = msgpack.encode(body);
        this.socket.write(payload);
      }
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
