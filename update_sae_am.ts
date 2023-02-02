// This script is used to update the AM and SAE details for all stores in the data.csv
// The data is read from a CSV file and the AM and SAE details are updated for each store.
// The script should be run from "src/scripts/data.csv"
// Data format in CSV file: [store_name,store_id,am,am_ph,am_id,sae,sae_ph,sae_id]

import * as fs from 'fs';
import * as csv from 'csv-parse';
import { UserDetails } from '../store-partner-api-lambda/dao/model/store-model';
import { storePartnerProfileAPI } from '../store-partner-api-lambda/handlers/store-partner-profile';
import { storeAPI } from '../store-partner-api-lambda/handlers/store';
import { storePartnerDAO } from '../store-partner-api-lambda/dao/store-partner-dao';

interface IRecord {
    store_name: string;
    store_id: string;
    am: string;
    am_ph: string;
    am_id: string;
    sae: string;
    sae_ph: string;
    sae_id: string;
}

interface IStoreUpdate {
    [storeId: string]: {
        amId: UserDetails;
        saeId: UserDetails;
    }
}


async function getDataFromCSV(): Promise<[IStoreUpdate, { [key: string]: string }]> {
    console.log('Reading data from CSV file...');
    const data: any = await new Promise((resolve, reject) => {
        fs.readFile('src/scripts/data.csv', 'utf-8', (err, data) => {
            if (err) reject(err);
            resolve(data);
        });
    });
    console.log('Reading data from CSV file...Done');

    console.log('Parsing data from CSV file...');
    const records: IRecord[] = await new Promise((resolve, reject) => {
        csv.parse(data, { columns: true }, (err, records) => {
            if (err) reject(err);
            resolve(records as IRecord[]);
        });
    });
    console.log(`Parsing data from CSV file...Done. Found ${records.length} records.`);

    let storeData: IStoreUpdate = {};
    let amSaeData: { [key: string]: string } = {}

    records.forEach(record => {
        // create a dict(storeID: {}) of dicts
        if (!storeData[record.store_id]) {
            storeData[record.store_id] = {
                amId: {
                    id: record.am_id,
                    name: record.am,
                    phoneNumber: record.am_ph
                },
                saeId: {
                    id: record.sae_id,
                    name: record.sae,
                    phoneNumber: record.sae_ph
                }
            }
        }
        if (!amSaeData[record.am_id]) {
            amSaeData[record.am_id] = record.am;
        }
        if (!amSaeData[record.sae_id]) {
            amSaeData[record.sae_id] = record.sae;
        }
    });
    // console.log(amSaeData);


    // console.log(storeData);

    return [storeData, amSaeData];
}

async function main() {
    let [storeData, storePartners] = await getDataFromCSV();
    console.log(`Found ${Object.keys(storeData).length} Stores.`);
    console.log(`Found ${Object.keys(storePartners).length} Store Partners.`);

    console.log("Removing all stores from all store partners...");
    for (const storePartnerId in storePartners) {
        console.log(`Store Partner ID: ${storePartnerId} Name: ${storePartners[storePartnerId]}`);
        let storePartner = await storePartnerDAO.getStorePartner(storePartnerId)
        await storePartnerProfileAPI.updateStorePartner({ storePartnerId: storePartnerId, stores: [] }, { clientId: 'am-sae-update', userId: 'am-sae-update', storePartner: storePartner });
    }
    console.log("Removing all stores from all store partners...Done");

    console.log("Adding stores to store partners...");
    for (const storeId in storeData) {
        console.log(`Store ID: ${storeId} Name: ${storeData[storeId].amId.name} ${storeData[storeId].saeId.name}`);
        await storeAPI.updateStore({ storeId: storeId, saeDetails: storeData[storeId].saeId, areaManager: storeData[storeId].amId });
    }
    console.log("Adding stores to store partners...Done");
}

main();