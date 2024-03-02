import {expect,test} from "bun:test"

async function runScript(commands){
    const proc = Bun.spawn(["./db"],{stdin:"pipe"})
    commands.forEach((command)=>{
        proc.stdin.write(`${command}\n`)
    })
    proc.stdin.flush()
    proc.stdin.end()
    const output = (await new Response(proc.stdout).text()).split("\n")
    return output
}

test("Check for command 1",async()=>{
    const commands = ["insert 1 user1 person1@example.com",
    "select",
    ".exit",]

    const ouput = await runScript(commands)

    expect(ouput).toEqual(["db >Executed.",
    "db >(1, user1, person1@example.com)",
    "Executed.",
    "db >"])
})

test("Check for long values",async()=>{
    const long_user = "a".repeat(32)
    const long_email = "a".repeat(255)
    const commands = [`insert 1 ${long_user} ${long_email}`,"select",".exit"]

    const output = await runScript(commands)

    expect(output).toEqual([
        "db >Executed.",
        `db >(1, ${long_user}, ${long_email})`,
        "Executed.",
        "db >"
    ])
})

test("Check of overflow values",async()=>{
   const long_username = "a".repeat(33)
  const long_email = "a".repeat(256)
  const commands = [
    `insert 1 ${long_username} ${long_email}`,
    "select",
    ".exit",
  ]

  const output = await runScript(commands)

  expect(output).toEqual([
    "db >String too long.",
    "db >Executed.",
    "db >",
  ])
})

test("2+2",()=>{
    expect(2+2).toBe(4)
})