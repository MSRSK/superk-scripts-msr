import fetch from "node-fetch";
// import AWS from "aws-sdk";

import csv from 'csv-parser';
import { promises as ps } from 'fs';
import fs from "fs";


let created_products = {}
let created_products_count = 0
let created_variants_count = 0
let pro_start = ''
let var_start = ''
// let ERP_ACCESS_TOKEN = "token 9264f4b80081cc0:d7db3e13a6e8e12" //gamma
let ERP_ACCESS_TOKEN = "token b50ef12084e4ea2:b3647be6cce8df7" // erp
let SPA_HEADER = {}
var erpHeader = { "Authorization": ERP_ACCESS_TOKEN }

let erpRequestOptions = {
  method: 'GET',
  headers: erpHeader,
  redirect: 'follow'

}
fetch("https://erp.superk.in/api/method/superk.superk.utils.get_spa_access_token", erpRequestOptions)
  .then(response => response.json())
  .then(async (result) => {
    let token = result["message"];
    SPA_HEADER = { "Authorization": token }
    console.log(SPA_HEADER);
    await create_products()
    create_variants()
    // update_variants()
  })
let create_products = () => {
  return new Promise(async (resolve, reject) => {
    let products = JSON.parse(await ps.readFile("/Users/msr/Documents/SuperK/dev/Junk/DynamoDB/data_products.json"));
    products = products["data"];
    let l = products.length;
    console.log(`${l} products to create`);
    let created = 0
    pro_start = new Date().getTime()

    let productGroups = [], size = 600;

    for (let i = 0; i < products.length; i += size) {
      productGroups.push(products.slice(i, i + size));
    }

    let errorProducts = []

    for (let i = 0; i < productGroups.length; i++) {//
      let requests = []
      let products = productGroups[i]
      for (let i = 0; i < products.length; i++) {
        let product = {
          name: products[i]["name"],
          erpId: products[i]["erpId"],
          categories: products[i]["categories"],
          brand: products[i]["brand"]
        }

        let requestOptions = {
          method: 'POST',
          body: JSON.stringify(product),
          headers: SPA_HEADER,
          redirect: 'follow'
        };
        let promiseResponse = fetch("https://1kb5gewg21.execute-api.ap-south-1.amazonaws.com/prod/product", requestOptions)
        requests.push(promiseResponse)
      }
      let responses = await Promise.all(requests)
      for (let i = 0; i < responses.length; i++) {
        let response = responses[i]
        if (response.ok) {
          let result = await response.json()
          if (result.erpId) {
            console.log(`${i}/${l} || ${result.erpId} and ${result.id}`)
            created_products[result.erpId] = result.id
            created_products_count++;
          } else {
            console.log(result)
          }
        } else {
          console.log(`error response = ${response.url}`)
          errorProducts.push(response.url)
        }
      }
    }

    console.log("=======================================================================================")
    console.log(`Creation Time: ${(new Date().getTime() - pro_start) / 600} s 🤘`);
    console.log("=======================================================================================")
    resolve();
  });
}


let create_variants = () => {
  console.log("Creating Variants")
  var variants = []
  let errorVariants = []
  let productToVariant = {}
  fs.createReadStream('/Users/msr/Documents/SuperK/dev/Junk/DynamoDB/data_variants.csv')
    .pipe(csv())
    .on('data', (data) => {
      data.bundle_quantity = parseInt(data.bundle_quantity)
      data.max_qty = parseInt(data.max_qty)
      data.mrp = parseFloat(data.mrp)
      data.selling_price = parseFloat(data.selling_price)
      variants.push(data)

      if (!productToVariant[`${data.alias}`]) {
        productToVariant[`${data.alias}`] = []
      }
      productToVariant[`${data.alias}`].push(data)
    })
    .on('end', async () => {
      let l = variants.length;
      let variantGroups = []
      let values = Object.values(productToVariant).sort((e1, e2) => e2.length - e1.length)
      let total = 0;
      values.forEach(value => {
        total += value.length
      })
      console.log(total)
      console.log(values[0].length)
      total = 0;
      let loop = values[0].length
      for (let i = 0; i < loop; i++) {
        let group = []
        for (let j = 0; j < values.length; j++) {
          let variant = values[j].pop()
          if (variant) {
            group.push(variant)
          }
        }
        total += group.length
        variantGroups.push(group)
      }

      let productGroups = [], size = 600;
      for (let i = 0; i < variantGroups.length; i++) {
        if (variantGroups[i].length > size) {
          for (let j = 0; j < variantGroups[i].length; j += size) {
            productGroups.push(variantGroups[i].slice(j, j + size));
          }
        } else {
          productGroups.push(variantGroups[i])
        }
      }
      variantGroups = productGroups
      console.log(`${l} variants to create`);
      var_start = new Date().getTime()
      for (let i = 0; i < variantGroups.length; i++) {
        let requests = []
        let variants = variantGroups[i]
        for (let i = 0; i < variants.length; i++) {
          let variant = {
            barcode: variants[i]["barcode"],
            erpId: variants[i]["erpId"],
            sellingPrice: variants[i]["selling_price"],
            mrp: variants[i]["mrp"],
            maxQty: variants[i]["max_qty"],
            bundleQty: variants[i]["bundle_quantity"],
            pickingCategory: variants[i]["picking_category"],
            productId: created_products[variants[i]["alias"]],
          }
          if (variant["productId"]) {
            var requestOptions = {
              method: 'POST',
              body: JSON.stringify(variant),
              headers: SPA_HEADER,
              redirect: 'follow'
            };
            let request = fetch(`https://1kb5gewg21.execute-api.ap-south-1.amazonaws.com/prod/product/${variant["productId"]}/variant`, requestOptions)
            requests.push(request)
          }
        }
        let responses = await Promise.all(requests)
        console.log("Upload successful")
        for (let i = 0; i < responses.length; i++) {
          let response = responses[i]
          if (response.ok) {
            let result = await response.json()
            if (result.erpId) {
              // console.log(`${i}/${l} || ${result.id} || ${result.productId} || ${result.erpId}`)
              created_variants_count++;
            } else {
              console.log(result)
            }
          } else {
            let result = await response.json()
            console.log(`error response = ${JSON.stringify(result, undefined, 2)}`)
            errorVariants.push(response.url)
          }
        }
        console.log(`errors till now = ${errorVariants.length}`)
      }

      console.log(`error variants = ${JSON.stringify(errorVariants, undefined, 2)}`)

      console.log("=======================================================================================")
      console.log(`Creation Time: ${(new Date().getTime() - var_start) / 600} s`);
      console.log("=======================================================================================")
      console.log("Yayy!!! Products & Variants Created Successfully 🤘")
      console.log(`Created ${created_products_count} products`)
      console.log(`Created ${created_variants_count} variants`)
    })
}

let update_variants = () => {
  console.log("Creating Variants")
  var variants = []
  fs.createReadStream('/Users/msr/Documents/SuperK/dev/GitHub/store-partner-server/update_variants.csv')
    .pipe(csv())
    .on('data', (data) => variants.push(data))
    .on('end', async () => {
      let l = variants.length;
      console.log(`${l} variants to update`);
      for (let i = 0; i < l; i++) {
        if (i == 0) var_start = new Date().getTime()

        let variant = {
          bundleQty: variants[i]["bundle_qty"],
        }
        var requestOptions = {
          method: 'PUT',
          body: JSON.stringify(variant),
          headers: SPA_HEADER,
          redirect: 'follow'
        };
        let response = await fetch(`https://1kb5gewg21.execute-api.ap-south-1.amazonaws.com/prod/product/${variants[i]["product_id"]}/variant/${variants[i]["id"]}`, requestOptions)
        if (response.ok) {
          let result = await response.json()
          if (result.erpId) {
            console.log(`${i}/${l} || ${result.id} || ${result.productId} || ${result.erpId}`)
            created_variants_count++;
          } else {
            console.log(result)
          }
        } else {
          console.log("===============ERROR==================")
          console.log(response)
        }

        if (i == l - 1) {
          console.log("=======================================================================================")
          console.log(`Creation Time: ${(new Date().getTime() - var_start) / 600} s`);
          console.log("=======================================================================================")
          console.log("Yayy!!! Variants Updated Successfully 🤘")
          console.log(`Updated ${created_variants_count} variants`)
        }
      }
    })
}