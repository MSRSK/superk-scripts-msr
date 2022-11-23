import fetch from "node-fetch";
import pkg from 'aws-sdk';
const { config, DynamoDB } = pkg;


async function createSKUsInSOSynchronous() {
    let batchSize = 1000; // Each API will take <batchSize> items
    let skipCount = -1; // Skip first <skipCount> items
    let breakLoop = 100 * batchSize + skipCount; // Break after <breakLoop> iterations
    let skipCounter;

    let batchPromises = [];

    let pro_start = new Date().getTime();
    let startTime;
    let skuArray = await getSKUArray();
    console.log(`Total Products to be created = ${skuArray.length}`); 

    let body = {
        "warehouse": getWarehouse(),
        "skus": []
    };

    for (let i = 0; i < skuArray.length; i++) {
        if (i <= skipCount) continue;
        if (i%1000 === 0) console.log(`Time after ${i} = ${(new Date().getTime() - pro_start)/1000} seconds`);

        body["skus"].push(skuArray[i]);
        
        if (body["skus"].length === batchSize || i+1 === skuArray.length) {
            const result = createSKUs(body);
            batchPromises.push(result);
            body["skus"] = [];
        }
        if (i === breakLoop || i+1 === skuArray.length) {
            startTime = new Date().getTime();
            console.log("Waiting for all promises to resolve");
            let responses = await Promise.all(batchPromises)
            for (let i = 0; i < responses.length; i++) {
                let response = responses[i];
                if (response.ok) {
                    console.log(`Result: ${JSON.stringify(await response.json())}`);
                }
                
            }
            console.log(`Time taken for all Promises to resolve = ${(new Date().getTime() - startTime)/1000} seconds`);
            skipCounter = i;
            break;
        }
    }
    console.table({
        "Time taken for all Promises to resolve": `${(new Date().getTime() - startTime)/1000} seconds`,
        "Promise Size": batchSize,
        "Total Promises": batchPromises.length,
        "Time taken for all Promises to resolve": `${(new Date().getTime() - startTime)/1000} seconds`,
        "Total Time taken for": `${(new Date().getTime() - pro_start)/1000} seconds 🤘`,
        "Next Skip Count": skipCounter
    })
}

async function scanWholeTable(tableName, dictKey) {
    config.update({ region: "ap-south-1" });
    var ddb = new DynamoDB({ apiVersion: "2012-08-10" });

    const initParams = {
        Select: "ALL_ATTRIBUTES",
        TableName: tableName,
    };
    let itemsData;
    if (dictKey) itemsData = {}
    else itemsData = []
    while (true) {
        const data = await ddb.scan(initParams).promise();
        data.Items.forEach((item) => {
            item = DynamoDB.Converter.unmarshall(item);
            if (dictKey) {
                if (Object.keys(itemsData).includes(item[dictKey])) {
                    itemsData[item[dictKey]].push(item);
                } else {
                    itemsData[item[dictKey]] = [item];
                }
            } else {
                itemsData.push(item);
            }
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

async function getSKUArray() {
    // JOIN Product and variant map
    const productArray = await scanWholeTable("Products", false);
    const variantMap = await scanWholeTable("Variants", "productId");
    let soSKUsArray = [];

    productArray.forEach((product) => {
        const variants = variantMap[product.id] ?? [];
        const sku = {
            sku_code: product.id,
            sku_desc: product.name,
            threshold_quantity: 1,
            ean_number: [...new Set(variants.map((variant) => variant.barcode))].join(","),
            scan_picking: "Scanable"
        };
        soSKUsArray.push(sku);
    });
    
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

function getSOAccessToken() {
    return "F31CHXDy2Tu7eLuBf24uRgVBI2GQy9";
}

function getWarehouse() {
    return "demo_superk_blr";
}

await createSKUsInSOSynchronous();