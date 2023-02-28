const Consumer = require('./consumer/index')

const maxBatchSize = 10
let uncommitedOffset = null

async function batchHandler ({batch, resolveOffset}) {
    const messageQuantity = batch.messages.length
    console.log(`CONSUMER Received batch, messages: ${messageQuantity} | Max Batch Size: ${maxBatchSize}`)

    if (messageQuantity < maxBatchSize) {
        if (!uncommitedOffset) {
            uncommitedOffset = batch.lastOffset()
        }

        console.log('Uncommited messages start at offset: ', uncommitedOffset)
        
        console.log(`CONSUMER ${messageQuantity} messages, waiting for more...`)
        return
    }

    // Process messages
    // we need to remove first n elements from the array and store them
    let sfBatch = batch.messages.splice(0, maxBatchSize)
    console.log('sfBatch', sfBatch)

    // const sfRes = await processMessages({ messages: batch.messages.slice(-maxBatchSize) })

    // for (const message of sfRes) {
    //     resolveOffset(message.offset)
    //     console.log(`Message ${message.value} processed`)
    // }

    // resolve(sfRes)
}

async function main () {
    const consumer = await Consumer()

    await consumer.run({
        eachBatchAutoResolve: false,
        eachBatch: async ({
            batch, resolveOffset, heartbeat,
            commitOffsetsIfNecessary, uncommittedOffsets,
            isRunning, isStale, pause
        }) => {
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
            
            // Clear interval
            clearInterval(heartBeatInterval)
        },
    });
}

main()