# Event Simulator

## Context
The final goal is to simulate multiple microservices consuming and emitting events.

The simulated environment consists of 4 Services:
- Game - Handles player registration, game creation, starting and stopping of games and players joining)
- Trading - Handles bank accounts for each player in each game. One bank account per player, per game. Also provides the functionality of buying "vouchers". These are events that permit a player to construct or upgrade robots.
- Robot - Handles Map movements, Loading and Selling of Cargo & fighting.
- Map - Instantiates the Map, provides information about resource distribution and excavates resources to be loaded by roboters.

Then there are player Clients (later developed by students that are "playing" the game). these clients send "intents" more commonly known as commands, to the core services to perform actions. E.g. "buying a construct-robot-voucher" as seen in input/voucher/buy-robot-intent.json.
All services communicate primarily via Events. Some of them happen naturally in order, but there are no guarantees - as is usual for event-driven architectures. Common flows look like this: Buying a new robot: "buy-robot"(player), "credits-withdrawn"(trading), "construct-robot-voucher-issued" (trading), "robot-constructed" (robot). The Mining / loading flow is as follows:
"excavate-resources" (player), "resource-excavated"(map), "resource-picked-up"(robot).
The selling flow would be:
"sell-resources" (player), "robot-delivered-resources"(robot), "credits-deposited"(trading)
Moving looks like:
"move-robot" (player), "robot-moved"(robot)

## Use cases

The simulator has the following usecases:
- A developer tests a single services integration with the whole system
- A developer is using the mock system to generate events for building prototype streaming pipelines that rely on sample data
- A devop is loadtesting a component, e.g. one service and database to benchmark how fast it can consume and produce events.
  In that case, the kafkaProducer produces events as fast as possible

## Technical requirements

Derived technical capabilities:
- Fire a chain of events in a sensible order. E.g. `game.created`, `player.joined`. It is important that the GameId does not change between these events. The same is true for many flows.
- Fire a single event for testing purposes.
- Usually there are two modes: Creating games and playing a single game.

Chains and single events should both be able to be fired in a continuous mode using virtual threads.
There should be the option to fire multiple chains for a single game and player.
E.g: Buying robots, Moving those robots, mining, selling, repeat.

There should be a "Game-Simulation mode" where a normal game is simulated and multiple chains for multiple players
in a single game are published continiously. They should be somewhat consistent.

That means that we need a way to build logical event chains. Use Classes to define "Chains".
Chains should be easy to define, so they should resemble something like an ordered List, 
where the programmer can define new chains.
E.g.
```
1. BuyRobotIntent
2. CreditsWithdrawn - needs to know how much all robots cost
3. VoucherIssued - needs to know how many robots
4. RobotConstructed - needs to know how many robots
```
Most importantly for these chains, is that they need to persist information between events. E.g. GameId, PlayerId, BankAccountId and potentially other values
like Credit / Resource amounts.
Another example, this time using an intent as trigger:
```
1. SellResourceIntent
2. RobotDeliveredResources - needs to specify which resources
3. CreditsDeposited - needs to calculate how many credits are added
```

Some chains can theoretically have any order in between. E.g:
```
1. GameCreated
2. PlayerJoined
2. BankAccountOpened
2. MapInitialized
3. GameStarted
```
For simplicity they will still be executed in order.

To summarize: Chains need to store information while being published (Afterwards they can be cleared)
All form of Ids: GameId, PlayerId, BankaccountId, RobotId, MineId...
Values for the specific workflow:
- Resource amounts for mining and selling
- Credit amounts for buying and selling

Game creation chains can immediately be cleared, but playing a single game requires each virtual user to have persistent
money and robots.
In the game play mode, we need to store:
- Robots (Ids and positions and static dummy fields)
- Player balances (as a concurrent hashmap)
- GameId
- PlayerIds


## GameState

Game state is always just a tool to persist state between event chains. It is not there to really simulate a complete game
but achieve the bare minimum of "consistency". E.g. if a player buys 3 robots, they should pay the appropriate amount.
Other rules like this:
- Robots should only move to valid spaces, meaning not into voids.
- If a player Sells 5 plasma cores, they should be credited with money for 5 plasma cores, and not a random amount.
- If a robot is destroyed, it should not be able to move afterwards
- If a robot is bought, it should be able to be moved by the player afterwards
- Players should only be able to move their own robots.
- A player joining a game will then issue their intents in that same game (same UUID).
- Generally, IDs are persistent and dont change (BankAccount, Game, Mine, Player, Robot) participant ids are irrelevant

Generally, after a chain is completed, the game state should be updated appropriately to reflect the "changes" that
one published in the chain. But the gamestate is not the driving component publishing the events. The publishing 
of Chains is the main Goal and requirement of this kafkaProducer