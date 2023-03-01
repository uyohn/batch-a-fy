const Consumer = require('./consumer/index')

const maxBatchSize = 10
const minSleepInterval = 1000
const maxSleepInterval = 60000
let sleepInterval = minSleepInterval

// sleep function
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms))

async function batchHandler ({batch, resolveOffset}) {
    const messageQuantity = batch.messages.length
    console.log(`CONSUMER Received batch, messages: ${messageQuantity} | Max Batch Size: ${maxBatchSize}`)

    // if the sleep interval goes over certain threshold, process whatever there is
    if (sleepInterval > maxSleepInterval) {
        console.log('CONSUMER Max sleep interval reached, processing whatever amount of messages there is...')

        // turn this into separate function, reuse in other places
        for (const message of batch.messages) {
            resolveOffset(message.offset)
            console.log(`Message ${message.value} commited`)
        }

        // reset sleepInterval, so that it doesn't keep growing
        sleepInterval = minSleepInterval
        return
    }

    // if there are less messages than maxBatchSize, wait for more
    if (messageQuantity < maxBatchSize) {        
        console.log(`CONSUMER has ${messageQuantity} messages, waiting for more, sleep interval ${sleepInterval}...`)
        
        // sleep for sleepInterval
        await sleep(sleepInterval)

        // double sleepInterval
        sleepInterval = sleepInterval * 2

        return
    }

    // Process messages in batches of maxBatchSize
    for (let i = 1; i < messageQuantity / maxBatchSize; i++) {
        // get batch of messages, amount of messages is maxBatchSize
        const sfBatch = batch.messages.splice(0, maxBatchSize)

        // call sf
        // ...
        
        // after sf call resolveOffset for each message
        for (const message of sfBatch) {
            resolveOffset(message.offset)
            console.log(`Message ${message.value} commited`)
        }

        // reset sleepInterval
        sleepInterval = minSleepInterval
    }
}

let i = 0

async function main () {
    const consumer = await Consumer()

    await consumer.run({
        eachBatchAutoResolve: false,
        eachBatch: async ({
            batch, resolveOffset, heartbeat,
            commitOffsetsIfNecessary, uncommittedOffsets,
            isRunning, isStale, pause
        }) => {
            console.log(`CONSUMER Running batch ${i++}`)

            // if the consumer is shutting down in the middle of the batch,
            // the remaining messages won't be resolved and therefore not committed
            if (!isRunning() || isStale()) return

            // Send heartbeat to keep connection alive every 5 seconds
            const heartBeatInterval = setInterval(async () => {
                console.log('CONSUMER Sending heartbeat')
                await heartbeat()
            }, 5000)
            
            // Process batch
            await batchHandler({batch, resolveOffset})
            
            // Clear interval after batch is processed
            clearInterval(heartBeatInterval)
        },
    });
}

main()