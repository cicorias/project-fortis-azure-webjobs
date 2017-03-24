"use strict"

let request = require('request');
let parallelAsync = require('async/parallel');

const host = process.env.REACT_APP_SERVICE_HOST;
const siteFragment = `fragment FortisSiteDefinitionView on SiteCollection {
                                    sites {
                                        properties {
                                            targetBbox
                                            supportedLanguages
                                        }
                                    }
                        }`;
const twitterFragment = `fragment FortisTwitterAcctView on TwitterAccountCollection {
                                    accounts {
                                            accountName
                                            consumerKey
                                            token
                                            consumerSecret
                                            tokenSecret
                                    }
                        }`;
const locationsFragment = `fragment FortisDashboardLocationEdges on LocationCollection {
                                        edges {
                                            name
                                            coordinates
                                        }
                                    }`;

const query = ` ${siteFragment}
                        ${twitterFragment}
                        query Sites($siteId: String!) {
                            siteDefinition: sites(siteId: $siteId) {
                                ...FortisSiteDefinitionView
                            }
                            twitterAccounts: twitterAccounts(siteId: $siteId) {
                                ...FortisTwitterAcctView
                            }
                        }`;

const locationsQuery = ` ${locationsFragment}
                        query FetchLocations($siteId: String!) {
                            locations: locations(site: $siteId) {
                                ...FortisDashboardLocationEdges
                            }
                        }`;

const ResponseHandler = (error, response, body, callback) => {
    if(!error && response.statusCode === 200 && body.data) {
        callback(undefined, body.data);
    }else{
        const errMsg = `[${error}] occured while processing message request`;
        callback(errMsg, undefined);
    }
};

module.exports = {
    getSiteDefintion(siteId, webjobCB){
        const variables = {siteId};
        const SITE_POST = {
            url : `${host}/api/settings`,
            method : "POST",
            json: true,
            withCredentials: false,
            body: { query, variables }
        };

        const LOCATIONS_POST = {
            url : `${host}/api/edges`,
            method : "POST",
            json: true,
            withCredentials: false,
            body: { query: locationsQuery, variables: variables }
        };

        parallelAsync({
            settings: callback => {
                    request(SITE_POST, (error, response, body) => ResponseHandler(error, response, body, callback));
            },
            locations: callback => {
                    request(LOCATIONS_POST, (error, response, body) => ResponseHandler(error, response, body, callback));
            }
        }, (error, results) => {
                if(!error && Object.keys(results).length === 2 && results.settings.siteDefinition.sites.length > 0 && results.settings.twitterAccounts.accounts.length > 0){
                   webjobCB(undefined, Object.assign({}, {
                                                          locations: results.locations.locations.edges,
                                                          settings: results.settings.siteDefinition.sites[0].properties,
                                                          twitterAccounts: results.settings.twitterAccounts.accounts}));
                }else{
                   const errMsg = `Encountered site defintion undefined error for site ${siteId}. Request error[${error}]`
                   webjobCB(errMsg, undefined);
                }
           }
        );
  }
}