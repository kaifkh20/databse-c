import {expect,test,beforeEach,before} from "bun:test"

beforeEach(()=>{
    Bun.spawn(["rm","-rf","test.db"])

})

async function runScript(commands){
    const proc = Bun.spawn(["./db","test.db"],{stdin:"pipe"})
    commands.forEach((command)=>{
        proc.stdin.write(`${command}\n`)
    })
    proc.stdin.flush()
    proc.stdin.end()
    const output = (await new Response(proc.stdout).text())

    await proc.exited

    return output
}

test("Check for command 1",async()=>{
    const commands = ["insert 1 user1 person1@example.com",
    "select",
    ".exit",]

    const output = await runScript(commands)

    const result = output.split("\n")

    expect(result).toEqual(["db >Executed.",
    "db >(1, user1, person1@example.com)",
    "Executed.",
    "db >"])
})

test("Check for long values",async()=>{
    const long_user = "a".repeat(32)
    const long_email = "a".repeat(255)
    const commands = [`insert 1 ${long_user} ${long_email}`,"select",".exit"]

    const output = await runScript(commands)

    const result = output.split("\n")

    expect(result).toEqual([
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

    const result = output.split("\n")



  expect(result).toEqual([
    "db >String too long.",
    "db >Executed.",
    "db >",
  ])
})

test("Check for negative id",async()=>{
    // const id = -1;
    const commands = [
        `insert -1 username email`,
        ".exit"
    ]

    const output = await runScript(commands)

    const result = output.split("\n")

    expect(result).toEqual([
        "db >ID must be positive.",
        "Executed.",
        "db >"
    ])
})


test("Persistance to disk",async()=>{

    

    const commands1 = ["insert 1 user1 person1@example.com",
    ".exit",
    ]

    const output1 =  await runScript(commands1)

    const result1 = output1.split("\n")

    

    expect(result1).toEqual([
        "db >Executed.",
        "db >",
        
    ])
    
    const commands2 = ["select",
    ".exit"]

    const output2 = await runScript(commands2)

    const result2 = output2.split("\n");

    expect(result2).toEqual([
        "db >(1, user1, person1@example.com)",
        "Executed.",
        "db >"
    ])


})

test("Prints Constants",async()=>{
    const commands = [".constants",".exit"]
    const output = await runScript(commands);
    const result = output.split("\n");

    expect(result).toEqual([
      "db >Constants:",
      "ROW_SIZE: 293",
      "COMMON_NODE_HEADER_SIZE: 6",
      "LEAF_NODE_HEADER_SIZE: 10",
      "LEAF_NODE_CELL_SIZE: 297",
      "LEAF_NODE_SPACE_FOR_CELLS: 4086",
      "LEAF_NODE_MAX_CELLS: 13",
      "db >",
    ])
})

test("Printing the structure of one node",async()=>{
    
    

    const commands = [
        "insert 1 user1 user1@gmail.com",
        "insert 2 user2 user2@gmail.com",
        "insert 3 user3 user3@gmail.com",
        ".btree",
        ".exit"
    ]

    const output = await runScript(commands)
    const result = output.split("\n")

    expect(result).toEqual(
        [
            "db >Executed.",
            "db >Executed.",
            "db >Executed.",
            "db >Tree:",
            "leaf (size 3)",
            "  - 0 : 1",
            "  - 1 : 2",
            "  - 2 : 3",
            "db >"
        ]
    )
})

test("2+2",()=>{
    expect(2+2).toBe(4)
})