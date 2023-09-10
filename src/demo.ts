import { SerfClient, SerfCmd } from "./index";

function logJSON(text: string, data: any) {
  console.log(text, JSON.stringify(data));
}

async function main() {
  try {
    const clent = new SerfClient("localhost", 7373);

    clent
      .event("members", (event: any) => logJSON("Member list:", event))
      .event("members-join", (event: any) => logJSON("Member join:", event))
      .event("members-leave", (event: any) => logJSON("Member left:", event));

    await clent.open();
    await clent.send(SerfCmd.handshake, { Version: 1 });

    const members = await clent.send(SerfCmd.members);
    logJSON("Member List Query:", members);

    const keys = await clent.send(SerfCmd.list_keys);
    logJSON("Key List Query:", keys);

    setTimeout(async () => {
      await clent.close();
    }, 30 * 1000);
  } catch (err) {
    console.error("Error:", err);
  }
}

main();
