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

test("2+2",()=>{
    expect(2+2).toBe(4)
})