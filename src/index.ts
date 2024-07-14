import { parse } from "csv-parse";
import path from "path";
import fs from "fs";

/***
 * bitget import logic from Parqet:
 *
 *     switch (r = {
                        broker: "bitpanda",
                        source: "csv",
                        shares: Oe((e = Te(e, r))[6]),
                        price: Oe(e[8]),
                        amount: Oe(e[4]),
                        currency: e[5].toUpperCase(),
                        fee: "-" !== e[12] ? Oe(e[12]) : 0,
                        tax: 0
                    },
                    e[10]) {
                    case "Cryptocurrency":
                        r.asset = {
                            identifier: e[7].toUpperCase(),
                            assetType: "Crypto"
                        };
 */

const BINANCE_MAP: Array<{
  pattern: RegExp;
  intoIndex: number;
  into: string;
  coerce?: (v: string) => string;
}> = [
  // "Transaction ID",Timestamp,"Transaction Type",In/Out,"Amount Fiat",Fiat,"Amount Asset",Asset,"Asset market price","Asset market price currency","Asset class","Product ID",Fee,"Fee asset",Spread,"Spread Currency"
  // T8123f94c-2580-4129-ae62-***********,2022-07-02T09:19:36+02:00,buy,outgoing,250.00,EUR,0.01329013,BTC,18810.95,EUR,Cryptocurrency,1,-,-,-,-

  {
    pattern: /Date.*UTC/,
    intoIndex: 1,
    into: "Timestamp",
  },
  {
    pattern: /Trading total/,
    into: "Amount Fiat",
    intoIndex: 4,
    coerce: (v) => parseFloat(v.replace(/[a-zA-Z]+$/, "")).toFixed(20),
  },
  {
    pattern: /Trading total/,
    into: "Fiat", // alias. Currency name
    intoIndex: 5,
    coerce: (v) => v.replace(/^[^a-zA-Z]+/, ""),
  },
  {
    pattern: /Order Amount/,
    intoIndex: 6,
    into: "Amount Asset",
    coerce: (v) => parseFloat(v.replace(/[a-zA-Z]+$/, "")).toFixed(20),
  },
  {
    pattern: /Order Amount/,
    intoIndex: 7,
    into: "Asset",
    // extract asset name from e.g. '323232BTC'. Pairs are hard to split.
    coerce: (v) => v.replace(/^[^a-zA-Z]+/, ""),
  },
  {
    pattern: /Average Price/,
    intoIndex: 8,
    into: "Asset market price",
    coerce: (v) => parseFloat(v).toFixed(20),
  },
  {
    pattern: /Side/,
    intoIndex: 2,
    into: "Transaction Type",
    coerce: (v) => {
      if (v === "BUY") return "buy";
      else if (v === "SELL") return "sell";
      else throw new Error("Unknown transaction type");
    },
  },
];

if (require.main === module) {
  const [inputPath] = process.argv.slice(2);
  if (inputPath === undefined) {
    console.error("No input path specified");
    process.exit(1);
  }

  convertBinanceToParqet(path.resolve(process.argv[2]));
}

function convertBinanceToParqet(inputPath: string) {
  const parser = parse(fs.readFileSync(inputPath, "utf8"), {
    delimiter: ",",
  });
  const orders: Array<string[]> = [];

  // Use the readable stream api to consume records
  parser.on("readable", function () {
    let record;
    while ((record = parser.read()) !== null) {
      orders.push(record);
    }
  });
  // Catch any error
  parser.on("error", function (err) {
    console.error(err.message);
  });

  parser.on("end", () => {
    const tableNames = orders.shift();
    const statusColumnIdx = tableNames.findIndex((t) => /Status/.test(t));
    if (statusColumnIdx === -1) {
      throw new Error("Could not find status column.");
    }

    const result: Array<string[]> = [];

    row: for (const row of orders) {
      const resultRow: string[] = [];
      for (let i = 0; i < row.length; i++) {
        if (row[statusColumnIdx] !== "FILLED") {
          console.error("Order not filled. Skipping", row[statusColumnIdx]);
          continue row;
        }

        const rowEntry = row[i];
        BINANCE_MAP.filter((f) => f.pattern.test(tableNames[i])).forEach(
          (p) => {
            resultRow[p.intoIndex] = p.coerce ? p.coerce(rowEntry) : rowEntry;
          },
        );
      }

      resultRow[10] = "Cryptocurrency";
      resultRow[12] = "-"; // No fees for now.

      // Skip USDT/USDC/EUR buys/sells. They rather flaw the import.
      if (
        resultRow[7] === "USDT" ||
        resultRow[7] === "USDC" ||
        resultRow[7] === "EUR"
      ) {
        continue row;
      }

      const currencyUsedToBuy = resultRow[5];
      currencySafetyCheck: if (currencyUsedToBuy.length > 3) {
        if (currencyUsedToBuy === "USDT" || currencyUsedToBuy === "USDC") {
          resultRow[5] = "USD";
          break currencySafetyCheck;
        }

        console.error("Skipping transaction id; problematic currency.", row[1]);
        continue row;
      }

      result.push(resultRow);
    }

    process.stdout.write(`
"Disclaimer: All data is without guarantee, errors and changes are reserved."
"Robot, 2000-27-04"
robot@gmail.com
"Account opened at: 3/1/22, 4:46 PM"
"Venue: Bitpanda"
"Reported by Bitpanda GmbH"\n`);
    process.stdout.write(
      `"Transaction ID",Timestamp,"Transaction Type",In/Out,"Amount Fiat",Fiat,"Amount Asset",Asset,"Asset market price","Asset market price currency","Asset class","Product ID",Fee,"Fee asset",Spread,"Spread Currency"\n`,
    );
    process.stdout.write(result.map((row) => row.join(",")).join("\n"));
  });
}
