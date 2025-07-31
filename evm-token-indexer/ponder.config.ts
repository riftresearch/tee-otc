import { createConfig } from "ponder";
import { erc20ABI } from "./abis/erc20ABI";

/*
export default createConfig({
  chains: {
    mainnet: {
      id: 1,
      rpc: process.env.PONDER_RPC_URL_HTTP_MAINNET,
      ws: process.env.PONDER_WS_URL_HTTP_MAINNET,
    },
    base: {
      id: 8453,
      rpc: process.env.PONDER_RPC_URL_HTTP_BASE,
      ws: process.env.PONDER_WS_URL_HTTP_BASE,
    },
    anvil: {
      id: 31337,
      rpc: process.env.PONDER_RPC_URL_HTTP_ANVIL,
      ws: process.env.PONDER_WS_URL_HTTP_ANVIL,
      disableCache: true,
    },
  },
  contracts: {
    cbBTC: {
      abi: erc20ABI,
      chain: {
        mainnet: {
          address: "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf",
          startBlock: 13142655,
        },
        base: {
          address: "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf",
          startBlock: 33395269,
        },
        anvil: {
          address: "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf",
        },
      },
    },
  },
});
*/

export default createConfig({
  chains: {
    evm: {
      id: Number(process.env.PONDER_CHAIN_ID),
      rpc: process.env.PONDER_RPC_URL_HTTP,
      ws: process.env.PONDER_WS_URL_HTTP,
      disableCache: process.env.PONDER_DISABLE_CACHE === "true",
    },
  },
  contracts: {
    cbBTC: {
      abi: erc20ABI,
      chain: {
        evm: {
          address: "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf",
          startBlock: Number(process.env.PONDER_CONTRACT_START_BLOCK ?? 0),
        },
      },
    },
  },
});
