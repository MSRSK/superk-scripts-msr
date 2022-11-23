import fetch from "node-fetch";
import pkg from 'aws-sdk';
const { config, DynamoDB } = pkg;

function getSOAccessToken() {
    return "iqJDnDEBz6mIMcAo1K5hsxSuuKVeIS";
}

function getWarehouse() {
    return "demo_superk_blr";
}

async function scanWholeTable(tableName, dictKey) {
    config.update({ region: "ap-south-1" });
    var ddb = new DynamoDB({ apiVersion: "2012-08-10" });

    const initParams = {
        Select: "ALL_ATTRIBUTES",
        TableName: tableName,
    };
    let itemsData = {};
    while (true) {
        const data = await ddb.scan(initParams).promise();
        data.Items.forEach((item) => {
            item = DynamoDB.Converter.unmarshall(item);
            itemsData[item[dictKey]] = item;
        });
        console.log(`${tableName}: Scanned ${data["ScannedCount"]} || Total ${Object.keys(itemsData).length}`);
        if (data.LastEvaluatedKey) {
            initParams.ExclusiveStartKey = data.LastEvaluatedKey;
        } else {
            break;
        }
    }
    return itemsData;
}

async function createSKUsInSOSynchronous() {
    let batchSize = 1; // Each API will take <batchSize> items
    let skipCount = -1; // Skip first <skipCount> items
    let breakLoop = 1000 * batchSize + skipCount; // Break after <breakLoop> iterations
    let skipCounter;

    let batchPromises = [];

    let pro_start = new Date().getTime();
    let skuArray = await getSKUArray();
    console.log(`Total Variants to be created = ${skuArray.length}`);

    let body = {
        "warehouse": getWarehouse(),
        "skus": []
    };

    for (let i = 0; i < skuArray.length; i++) {
        if (i <= skipCount) continue;
        if (i%1000 === 0) console.log(`Time after ${i} = ${(new Date().getTime() - pro_start)/1000} seconds`);

        body["skus"].push(skuArray[i]);
        
        if (body["skus"].length === batchSize) {
            const result = createSKUs(body);
            batchPromises.push(result);
            body["skus"] = [];
        }
        if (i == breakLoop || i+1 == skuArray.length) {
            let startTime = new Date().getTime();
            console.log("Waiting for all promises to resolve");
            let responses = await Promise.all(batchPromises)
            for (let i = 0; i < responses.length; i++) {
                let response = responses[i];
                if (response.ok) {
                    console.log(`Result: ${await response.text()}`);
                }
                
            }
            console.log(`Time taken for all Promises to resolve = ${(new Date().getTime() - startTime)/1000} seconds`);
            skipCounter = i;
            break;
        }
    }
    console.log("=======================================================================================");
    console.log(`Promise Size = ${batchSize} 😱 🔥`);
    console.log(`Total Promises = ${batchPromises.length} 🥳`);
    console.log(`Total time taken for = ${(new Date().getTime() - pro_start)/1000} seconds 🤘`);
    console.log(`Next Skip count = ${skipCounter}`);
    console.log("=======================================================================================");
}


async function createSKUsInSOAsynchronous() {
    //================================================================//
    let batchSize = 1; // Each API will take <batchSize> items
    let skipCount = -1; // Skip first <skipCount> items
    //===============================================================//
    let breakLoop = 100 * batchSize + skipCount; // Break after <breakLoop> iterations

    let apiTime = [];
    let successCount = 0;
    let failureCount = 0;
    let skipCounter;

    let pro_start = new Date().getTime();
    let skuArray = await getSKUArray();
    console.log(`Total Variants to be created = ${skuArray.length}`);

    let body = {
        "warehouse": getWarehouse(),
        "skus": []
    };

    for (let i = 0; i < skuArray.length; i++) {
        if (i<=skipCount) continue;

        body["skus"].push(skuArray[i]);
        
        if (body["skus"].length === batchSize || i+1 == skuArray.length) {
            const startTime = new Date().getTime();
            
            const result = await createSKUs(body);
            if (result.status === 200) {
                successCount++;
                // console.log(`Result: ${await result.text()}`);
            } else {
                failureCount++;
                console.log(`-Result: ${await result.text()}`);
            }
            console.log(`Time taken = ${(new Date().getTime() - startTime)/1000} seconds`);

            apiTime.push((new Date().getTime() - startTime)/1000);
            body["skus"] = [];
        }
        if (i == breakLoop) {
            skipCounter = i;
            break;
        }

    }
    console.log(`Api Time = ${apiTime}`);
    console.log("=======================================================================================");
    console.log(`Promise Size = ${batchSize}`);
    console.log(`Success Count = ${successCount}`);
    console.log(`Failure Count = ${failureCount}`);
    console.log(`Average time taken for each API = ${(apiTime.reduce((a, b) => a + b, 0)/apiTime.length).toFixed(2)} seconds 😱 🔥`);
    console.log(`Total time taken for = ${(new Date().getTime() - pro_start)/1000} seconds 🤘`);
    console.log(`Next Skip count = ${skipCounter}`);
    console.log("=======================================================================================");
}

async function getSKUArray() {
    // JOIN Product and variant map
    const productMap = await scanWholeTable("Products", "id");
    const variantMap = await scanWholeTable("Variants", "id");
    const variantIds = Object.keys(variantMap);
    const soSKUsArray = [];
    variantIds.forEach((variantId) => {
        const variant = variantMap[variantId];
        const product = productMap[variant.productId];
        soSKUsArray.push({
            sku_code: variant.id,
            sku_desc: product.name,
            threshold_quantity: 1,
            ean_number: variant.barcode,
            mrp: variant.mrp,
            product_id: variant.productId,
        })
    })
    return soSKUsArray;
}


function createSKUs(body) {
    const accessToken = getSOAccessToken();
    const requestOptions = {
        method: "POST",
        body: JSON.stringify(body),
        headers: {
            "Content-Type": "application/json",
            Authorization: `${accessToken}`,
        },
        redirect: "follow",
    };
    const requestPromise = fetch(
        "https://pro.stockone.com/api/v1/products/",
        requestOptions
    );
    return requestPromise
}

createSKUsInSOSynchronous()