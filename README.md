# Assignment 2: The Socket Exchange

This is our implementation of the exchange server, trader client and market-data client.

## Language / runtime

- Language: Python 3
- We used the standard library only (`socket`, `threading`, `select`). No extra pip packages.
- Tested with `python3` on FreeBSD 14.4. Also works on Linux.
- There is nothing to compile.

## How to prepare

On FreeBSD, if `python3` is missing:

    pkg install python3

Make the launchers executable (once):

    chmod +x server/run-server client/run-trader client/run-market-data

All commands below should be run from the root of this folder (the one that has `server/`, `client/` and `src/`).

## How to start the Exchange Server

    ./server/run-server 127.0.0.1 5000

It listens on the host and port you pass in. Leave this process running. Logs go to stderr.

You can use another port if 5000 is taken, just use the same port for the clients.

## How to start a Trader Client

    ./client/run-trader 127.0.0.1 5000 alice

Arguments are: host, port, username.

The client connects and sends `LOGIN <username>` by itself. After that you can type commands on stdin, for example:

    BUY JNST 100 238
    SELL IMCT 25 517
    CANCEL 0
    QUIT

Replies from the server (`OK`, `ORDER_ACCEPTED`, `BOUGHT`, `SOLD`, `ERROR`, ...) are printed to stdout.

Start another trader in a different terminal with a different username, e.g. `bob`.

## How to start a Market-Data Client

    ./client/run-market-data 127.0.0.1 5000 JNST

Arguments are: host, port, instrument (`JNST` or `IMCT`).

The client connects and sends `SUBSCRIBE <instrument>` by itself. It then prints `OK` and any `TRADE` updates for that instrument.

You can run more than one market-data client at the same time.

## Configuration

- No config files.
- Host/port are only command line arguments.
- We ran everything on loopback (`127.0.0.1`) inside one FreeBSD VM, like the handout says.
- Instruments supported: `JNST` and `IMCT`.

## Layout

    server/run-server
    client/run-trader
    client/run-market-data
    src/server.py
    src/trader.py
    src/market_data.py
    src/protocol.py

`protocol.py` is just the newline framing helpers. The launchers `exec` the python files in `src/` and pass all arguments through.

## Notes

- Server uses one thread per client connection.
- Matching is exact price only, same instrument, buy vs sell.
- Closing a trader does not cancel their leftover orders.
- Username has to be unique among traders that are connected right now.