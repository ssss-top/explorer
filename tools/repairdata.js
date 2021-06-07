require('../db.js');
var fs = require('fs');
const BigNumber = require('bignumber.js');
const _ = require('lodash');

const asyncL = require('async');
const Web3 = require('web3');

const ERC20ABI = require('human-standard-token-abi');

const fetch = require('node-fetch');

const mongoose = require('mongoose');
const etherUnits = require('../lib/etherUnits.js');
const { Market } = require('../db.js');

const Block = mongoose.model('Block');
const Transaction = mongoose.model('Transaction');
const Account = mongoose.model('Account');
const Contract = mongoose.model('Contract');
const TokenTransfer = mongoose.model('TokenTransfer');

const ERC20_METHOD_DIC = { '0xa9059cbb': 'transfer', '0xa978501e': 'transferFrom', '0x0d5f2659':'gbzzTransfer'};

const config = { nodeAddr: 'localhost', wsPort: 8546, bulkSize: 100 };
try {
    var local = require('../config.json');
    _.extend(config, local);
    console.log('config.json found.');
} catch (error) {
    if (error.code === 'MODULE_NOT_FOUND') {
        var local = require('../config.example.json');
        _.extend(config, local);
        console.log('No config file found. Using default configuration... (config.example.json)');
    } else {
        throw error;
        process.exit(1);
    }
}

console.log(`Connecting ${config.nodeAddr}:${config.wsPort}...`);

const web3 = new Web3(new Web3.providers.WebsocketProvider(`ws://${config.nodeAddr}:${config.wsPort.toString()}`));

const normalizeTX = async (txData, receipt, blockData) => {
    const tx = {
        blockHash: txData.blockHash,
        blockNumber: txData.blockNumber,
        from: txData.from.toLowerCase(),
        hash: txData.hash.toLowerCase(),
        value: etherUnits.toEther(new BigNumber(txData.value), 'wei'),
        nonce: txData.nonce,
        r: txData.r,
        s: txData.s,
        v: txData.v,
        gas: txData.gas,
        gasPrice: String(txData.gasPrice),
        input: txData.input,
        transactionIndex: txData.transactionIndex,
        timestamp: blockData.timestamp,
    };
    v = 0;
    try{
        v = receipt.gasUsed;
    }catch (e){
        console.log(`\t- block #${blockData.number.toString()}} gasUsed type error.`);
        v = 0 ;
    }

    tx.gasUsed = v;

    if (receipt.status) {
        tx.status = receipt.status;
    }

    if (txData.to) {
        tx.to = txData.to.toLowerCase();
        return tx;
    } else if (txData.creates) {
        tx.creates = txData.creates.toLowerCase();
        return tx;
    } else {
        tx.creates = receipt.contractAddress.toLowerCase();
        return tx;
    }
};

/**
 Write the whole block object to DB
 **/
var writeBlockToDB = function (config, blockData, flush) {
    const self = writeBlockToDB;
    if (!self.bulkOps) {
        self.bulkOps = [];
    }
    if (blockData && blockData.number >= 0) {
        self.bulkOps.push(new Block(blockData));
        if (!('quiet' in config && config.quiet === true)) {
            console.log(`\t- block #${blockData.number.toString()} inserted.`);
        }
    }

    if (flush && self.bulkOps.length > 0 || self.bulkOps.length >= config.bulkSize) {
        const bulk = self.bulkOps;
        self.bulkOps = [];
        if (bulk.length === 0) {
            console.log(`\t- block #${blockData.number.toString()} bulk.length.`);
            return;
        }

        Block.collection.insert(bulk, (err, blocks) => {
            if (typeof err !== 'undefined' && err) {
                if (err.code === 11000) {
                    if (!('quiet' in config && config.quiet === true)) {
                        console.log(`Skip: Duplicate DB key : ${err}`);
                    }
                } else {
                    console.log(`Error: Aborted due to error on DB: ${err}`);
                    process.exit(9);
                }
            } else {
                if (!('quiet' in config && config.quiet === true)) {
                    console.log(`* ${blocks.insertedCount} blocks successfully written.`);
                }
            }
        });
    }
};
/**
 Break transactions out of blocks and write to DB
 **/
const writeTransactionsToDB = async (config, blockData, flush) => {
    const self = writeTransactionsToDB;
    if (!self.bulkOps) {
        self.bulkOps = [];
        self.blocks = 0;
    }
    // save miner addresses
    if (!self.miners) {
        self.miners = [];
    }
    if (blockData) {
        self.miners.push({ address: blockData.miner, blockNumber: blockData.number, type: 0 });
    }
    if (blockData && blockData.transactions.length > 0) {
        for (d in blockData.transactions) {
            const txData = blockData.transactions[d];
            const receipt = await web3.eth.getTransactionReceipt(txData.hash);
            const tx = await normalizeTX(txData, receipt, blockData);
            // Contact creation tx, Event logs of internal transaction
            if (txData.input && txData.input.length > 2) {
                // Contact creation tx
                if (txData.to === null) {
                    // Support Parity & Geth case
                    if (txData.creates) {
                        contractAddress = txData.creates.toLowerCase();
                    } else {
                        contractAddress = receipt.contractAddress.toLowerCase();
                    }
                    const contractdb = {};
                    let isTokenContract = true;
                    const Token = new web3.eth.Contract(ERC20ABI, contractAddress);
                    contractdb.owner = txData.from;
                    contractdb.blockNumber = blockData.number;
                    contractdb.creationTransaction = txData.hash;
                    try {
                        const call = await web3.eth.call({ to: contractAddress, data: web3.utils.sha3('totalSupply()') });
                        if (call === '0x') {
                            isTokenContract = false;
                        } else {
                            try {
                                // ERC20 & ERC223 Token Standard compatible format
                                contractdb.tokenName = await Token.methods.name().call();
                                contractdb.decimals = await Token.methods.decimals().call();
                                contractdb.symbol = await Token.methods.symbol().call();
                                contractdb.totalSupply = await Token.methods.totalSupply().call();
                            } catch (err) {
                                isTokenContract = false;
                            }
                        }
                    } catch (err) {
                        isTokenContract = false;
                    }
                    contractdb.byteCode = await web3.eth.getCode(contractAddress);
                    if (isTokenContract) {
                        contractdb.ERC = 2;
                    } else {
                        // Normal Contract
                        contractdb.ERC = 0;
                    }
                    // Write to db
                    Contract.update(
                        { address: contractAddress },
                        { $set: contractdb },
                        { upsert: true },
                        (err, data) => {
                            if (err) {
                                console.log("Contract err", err);
                            }
                        },
                    );
                } else {
                    // Internal transaction  . write to doc of InternalTx
                    const transfer = {
                        'hash': '', 'blockNumber': 0, 'from': '', 'to': '', 'contract': '', 'value': 0, 'timestamp': 0,
                    };
                    const methodCode = txData.input.substr(0, 10);
                    if (ERC20_METHOD_DIC[methodCode] === 'transfer' || ERC20_METHOD_DIC[methodCode] === 'transferFrom') {
                        if (ERC20_METHOD_DIC[methodCode] === 'transfer') {
                            // Token transfer transaction
                            transfer.from = txData.from.toLowerCase();
                            transfer.to = `0x${txData.input.substring(34, 74)}`.toLowerCase();
                            transfer.value = Number(`0x${txData.input.substring(74)}`);
                        } else {
                            // transferFrom
                            transfer.from = `0x${txData.input.substring(34, 74)}`.toLowerCase();
                            transfer.to = `0x${txData.input.substring(74, 114)}`.toLowerCase();
                            transfer.value = Number(`0x${txData.input.substring(114)}`);
                        }
                        transfer.method = ERC20_METHOD_DIC[methodCode];
                        transfer.hash = txData.hash;
                        transfer.blockNumber = blockData.number;
                        transfer.contract = txData.to;
                        transfer.timestamp = blockData.timestamp;
                        // Write transfer transaction into db
                        TokenTransfer.update(
                            { hash: transfer.hash },
                            { $set: transfer },
                            { upsert: true },
                            (err, data) => {
                                if (err) {
                                    console.log("TokenTransfer err" ,err);
                                }
                            },
                        );
                    } else if ( ERC20_METHOD_DIC[methodCode] === 'gbzzTransfer' ) {
                        const typesArray = [
                            {type: 'uint256', name: 'totalPayout'},
                            {type: 'uint256', name: 'cumulativePayout'},
                            {type: 'uint256', name: 'callerPayout'},
                        ];
                        v = 0;
                        try{
                            v = web3.eth.abi.decodeParameters(typesArray, receipt.logs[1].data).totalPayout;
                        }catch (e){
                            console.log(`\t- block #${blockData.number.toString()}} decodeParameters type error.`);
                            v = 0 ;
                        }
                        transfer.value = v;
                        transfer.from = txData.from.toLowerCase();
                        transfer.to = `0x${txData.input.substring(34, 74)}`.toLowerCase();
                        transfer.method = ERC20_METHOD_DIC[methodCode];
                        transfer.hash = txData.hash;
                        transfer.blockNumber = blockData.number;
                        transfer.contract = txData.to;
                        transfer.timestamp = blockData.timestamp;
                        // Write transfer transaction into db
                        TokenTransfer.update(
                            { hash: transfer.hash },
                            { $set: transfer },
                            { upsert: true },
                            (err, data) => {
                                if (err) {
                                    console.log("TokenTransfer err", err);
                                }
                            },
                        );
                    }
                }
            }
            self.bulkOps.push(tx);
        }
        if (!('quiet' in config && config.quiet === true)) {
            console.log(`\t- block #${blockData.number.toString()}: ${blockData.transactions.length.toString()} transactions recorded.`);
        }
    }
    self.blocks++;

    if (flush && self.blocks > 0 || self.blocks >= config.bulkSize) {
        const bulk = self.bulkOps;
        self.bulkOps = [];
        self.blocks = 0;
        const { miners } = self;
        self.miners = [];

        // setup accounts
        const data = {};
        bulk.forEach((tx) => {
            data[tx.from] = { address: tx.from, blockNumber: tx.blockNumber, type: 0 };
            if (tx.to) {
                data[tx.to] = { address: tx.to, blockNumber: tx.blockNumber, type: 0 };
            }
        });

        // setup miners
        miners.forEach((miner) => {
            data[miner.address] = miner;
        });

        // const accounts = Object.keys(data);

        // if (bulk.length === 0 && accounts.length === 0) return;

        // update balances
        // if (config.settings.useRichList && accounts.length > 0) {
        //     asyncL.eachSeries(accounts, (account, eachCallback) => {
        //         const { blockNumber } = data[account];
        //         // get contract account type
        //         web3.eth.getCode(account, (err, code) => {
        //             if (err) {
        //                 console.log(`ERROR: fail to getCode(${account})`);
        //                 return eachCallback(err);
        //             }
        //             if (code.length > 2) {
        //                 data[account].type = 1; // contract type
        //             }
        //
        //             web3.eth.getBalance(account, blockNumber, (err, balance) => {
        //                 if (err) {
        //                     console.log(err);
        //                     console.log(`ERROR: fail to getBalance(${account})`);
        //                     return eachCallback(err);
        //                 }
        //
        //                 data[account].balance = parseFloat(web3.utils.fromWei(balance, 'ether'));
        //                 eachCallback();
        //             });
        //         });
        //     }, (err) => {
        //         let n = 0;
        //         accounts.forEach((account) => {
        //             n++;
        //             if (!('quiet' in config && config.quiet === true)) {
        //                 if (n <= 5) {
        //                     console.log(` - upsert ${account} / balance = ${data[account].balance}`);
        //                 } else if (n === 6) {
        //                     console.log(`   (...) total ${accounts.length} accounts updated.`);
        //                 }
        //             }
        //             // upsert account
        //             Account.collection.update({ address: account }, { $set: data[account] }, { upsert: true });
        //         });
        //     });
        // }

        if (bulk.length > 0) {
            bulk.forEach((tx) => {
                Transaction.collection.update({ hash: tx.hash }, { $set: tx }, { upsert: true });
            });
            // Transaction.collection.insert(bulk, (err, tx) => {
            //   if (typeof err !== 'undefined' && err) {
            //     if (err.code === 11000) {
            //       if (!('quiet' in config && config.quiet === true)) {
            //         console.log(`Skip: Duplicate transaction key ${err}`);
            //       }
            //     } else {
            //       console.log(`Error: Aborted due to error on Transaction: ${err}`);
            //       process.exit(9);
            //     }
            //   } else {
            //     if (!('quiet' in config && config.quiet === true)) {
            //       console.log(`* ${tx.insertedCount} transactions successfully recorded.`);
            //     }
            //   }
            // });
        }
    }
};
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
async function sleepMSecond(ms) {
    await sleep(ms);
    console.log(ms, "m seconds later");
}
/**
 修复缺失的块
 **/
const runPatcherBlocks = async (config, startBlock, endBlock) => {
    if (!web3 || !web3.eth.net.isListening()) {
        console.log('--runPatcherBlocks-----Error: Web3 is not connected. Retrying connection shortly...');
        setTimeout(() => {
            runPatcherBlocks(config);
        }, 3000);
        return;
    }
    if (typeof startBlock === 'undefined' || typeof endBlock === 'undefined') {
        endBlock = config.syncBlock;
    }
    if ( endBlock === config.startBlock){
        console.log('--runPatcherBlocks-----sync finish');
        return;
    }
    startBlock = endBlock - config.repairPatch;
    // const blockFind = Block.find({
    //   number: {
    //     $gt: startBlock,
    //     $lte: endBlock
    //   }
    // }, 'number').lean(true).sort('-number').limit(batchCount);
    // blockFind.exec(async (err, docs) => {
    //   if (err || !docs || docs.length < batchCount) {
    console.log('--runPatcherBlocks------start patch blocks.', endBlock);

    let patchBlock = startBlock;
    let count = 0;
    console.log(`--runPatcherBlocks-------Patching from #${startBlock} to #${endBlock}`);
    while (count < config.repairPatch && patchBlock <= endBlock) {
        if (!('quiet' in config && config.quiet === true)) {
            console.log(`--runPatcherBlocks-----fix Patching Block: ${patchBlock}`);
        }
        const ret = await web3.eth.getBlock(patchBlock, true, async (error, patchData) => {
            if (error) {
                console.log(`--runPatcherBlocks-----Warning: error on getting block with hash/number: ${patchBlock}: ${error}`);
            } else if (patchData === null) {
                console.log(`--runPatcherBlocks-----Warning: null block data received from the block with hash/number: ${patchBlock}`);
            } else {
                console.log("--runPatcherBlocks-----fix checkBlockDBExistsThenWrite Block: ", patchData.number);
                writeBlockToDB(config, patchData, true);
                await writeTransactionsToDB(config, patchData, true);
                console.log("--runPatcherBlocks-----fix checkBlockDBExistsThenWrite Block: end", patchData.number);
            }
        });
        await sleepMSecond(20);
        patchBlock++;
        count++;
    }

    endBlock = endBlock - config.repairPatch;
    console.log("--runPatcherBlocks-----setTimeout: runPatcher", endBlock);

    config["syncBlock"] = config.syncBlock - config.repairPatch;
    var jsonstr = JSON.stringify(config);
    fs.writeFile('./config.json', jsonstr, function(err) {
        if (err) {
            console.error(err);
        }else{
            console.log('----------update sync block config.json suc-------------');
        }
    });

    await sleepMSecond(10000);
    process.exit(0);

    // setTimeout(() => {
    //   runPatcherBlocks(config, startBlock, endBlock);
    // }, 1000);
    // } else {
    //   console.log(`--runPatcherBlocks-------ignore from #${startBlock} to #${endBlock}, length:  #${docs.length} `);
    //   endBlock = endBlock - batchCount;
    //   setTimeout(() => {
    //     runPatcherBlocks(config, startBlock, endBlock);
    //   }, 1000);
    // }
    // });
};

console.log('fixing blocks');
runPatcherBlocks(config);
console.log('fixing blocks end');
