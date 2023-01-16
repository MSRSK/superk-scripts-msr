import { mapper } from "../store-partner-api-lambda/dao/generic-dao";
import { PRODUCT_LEVEL_INDEX, RulesModel } from "../store-partner-api-lambda/dao/rules-engine/model/rules-model";
import { rulesDAO } from "../store-partner-api-lambda/dao/rules-engine/rules-dao";
import { getKey } from "../store-partner-api-lambda/dao/rules-engine/utils";

async function updateRule() {
    let rule = new RulesModel();
    const level = "PRODUCT";
    const value = "f9b7d019-8285-4e69-aca9-af74d92e1037";
    const maxLimit = 40;

    let productLevel = getKey({ level, value });
    let rules: RulesModel[] = [];

    for await (const rule of mapper.query(RulesModel, { productLevel }, {
        indexName: PRODUCT_LEVEL_INDEX,
        pageSize: 100
    })) {
        if (rule.locationLevel.startsWith("STORE") && rule.maxLimit) {
            if (rule.maxLimit.value < maxLimit) {
                console.log(`Rule to update: ${JSON.stringify(rule, undefined, 2)}`);

                rule.maxLimit.value = maxLimit;
                let updatedRule = await rulesDAO.update(rule);

                console.log(`Rule updated: ${JSON.stringify(updatedRule, undefined, 2)}`);
            } else {
                console.log(`Skipping rule as maxLimit is ${rule.maxLimit.value}`);
            }

        } else {
            console.log(`Skipping rule as locationLevel is ${JSON.stringify(rule, undefined, 2)}}`);
        }
    }
}

updateRule().then(() => {
    console.log("Done");
}).catch((e) => {
    console.log("Error: " + e);
});