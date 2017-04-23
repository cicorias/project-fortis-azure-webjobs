"use strict";

let azure = require('azure-storage');
let twitter = require('twitter');
let Services = require('./services');
let moment = require('moment');
let appInsights = require("applicationinsights");
let twitterClientPool = new Map();
let tweetCount = 0;

const queueService = azure.createQueueService();
const appInsightsClient = appInsights.getClient();
const MAXRECONNECTSALLOWED = 15;
const RECONNECT_DELAY_MS = 10000;
const MAX_TERMS_ALLOWED = 350;
const QUEUE = process.env.PRE_NLP_QUEUE;
const FORTIS_SITE_NAME = process.env.FORTIS_SITE_NAME;

if(!appInsightsClient){
  console.error("App Insight Instantiation Error");

  return;
}

function AddPooledConnection(connectionConfig){
    if(!connectionConfig){
      appInsightsClient.trackException(new Error("Twitter connection undefined error"));
    }

    try{
      console.log(JSON.stringify(twitterClientPool));
      twitterClientPool.set(connectionConfig.accountName, new twitter({
          consumer_key: connectionConfig.consumerKey,
          consumer_secret: connectionConfig.consumerSecret,
          access_token_key: connectionConfig.token,
          access_token_secret: connectionConfig.tokenSecret
        }));
    }catch(err){
      appInsightsClient.trackException(new Error("Twitter connection undefined error: " + JSON.stringify(connectionConfig)));
    }
};

function CreatePooledConnections(twitterClientConfigs){
  try{
    twitterClientConfigs.forEach(connection=>AddPooledConnection(connection));
  }catch(err){
    appInsightsClient.trackException(new Error("connection parsing error: " + JSON.stringify(twitterClientConfigs)));
  }
};

function ValidTweet(tweet, supportedLanguages){
  if(tweet.text && tweet.lang && supportedLanguages.indexOf(tweet.lang) > -1){
    tweetCount++;
    return true;
  }

  return false;
};

function processTweet(tweet, supportedLanguages){
  if(!ValidTweet(tweet, supportedLanguages)){
    return;
  }
  
  var iso_8601_created_at = null;
  
  try{
    iso_8601_created_at = moment(tweet.created_at, 'dd MMM DD HH:mm:ss ZZ YYYY', 'en');
  }catch(e){
      console.error(JSON.stringify(tweet));
  }
  
  var tweetEssentials= {};

  try{
    tweetEssentials = {
      // we use moment.js to pars the "strange"" Twitter dateTime format
      created_at: iso_8601_created_at,
      id: tweet.id_str || "N/A",
      user_id: tweet.user ? tweet.user.id : "unknown",
      geo: tweet.coordinates,
      originalSources: [tweet.user ? tweet.user.name : "twitter"],
      profileDescription: tweet.user && tweet.user.description ? tweet.user.description : "",
      lang: tweet.lang,
      message: tweet.text,
      retweet_count: tweet.retweeted_status ? tweet.retweeted_status.retweet_count || 0 : 0,
      retweet_id: tweet.retweeted_status ? tweet.retweeted_status.id_str : null
    };
  }catch(e){
    console.error('error');
    console.error(JSON.stringify(tweet));
  }

  var message = {
    source: 'twitter',
    created_at: iso_8601_created_at,
    message: tweetEssentials
  };
  
  queueService.createMessage(QUEUE, JSON.stringify(message), function(err, result, response) {
    if (err) {
      appInsightsClient.trackException(new Error("Unable to write tweet to queue"));
    }else{
      console.log(`Wrote [${JSON.stringify(message)}] to ${QUEUE}`);
    }
  });
}

function TrackStream(client, filter, connAttempts, clientName, supportedLanguages) {
  if(client){
      appInsightsClient.trackEvent("Stream connection opened", {filter: JSON.stringify(filter), clientName: clientName});
      console.log('Connected to ', clientName);
      console.log(`Queue [${QUEUE}]`);
      client.stream('statuses/filter', filter, stream => {
          stream.on('data', tweet => {
            if(tweetCount % 100 == 0){
              console.log(tweetCount);
              appInsightsClient.trackTrace("Starting Twitter StreamP", {tweetCount});
            }

            processTweet(tweet, supportedLanguages);
          });

          stream.on('error', error => {
            console.log(`streaming error [${error}]`);
            appInsightsClient.trackException(new Error("Twitter streaming error", error));
          });

          stream.on('end', error => {
              appInsightsClient.trackException(new Error("Twitter connection disconnected", client.consumerKey, error));
              
              if(connAttempts <= MAXRECONNECTSALLOWED){
                  ReConnectStream(client, filter, connAttempts, clientName, supportedLanguages);
              }
          });
        
          stream.on('destroy', error => {
              appInsightsClient.trackException(new Error("Twitter connection destroyed/disconnected", client.consumerKey, error));

              if(connAttempts <= MAXRECONNECTSALLOWED){
                  ReConnectStream(client, filter, connAttempts, clientName, supportedLanguages);
              }
          });
      });
  }
}

function ReConnectStream(originalClient, filter, connAttempts, clientName, supportedLanguages){
     AddPooledConnection({name: clientName, consumerKey: originalClient.options.consumer_key, consumerSecret: originalClient.options.consumer_secret, token: originalClient.options.access_token_key, tokenSecret: originalClient.options.access_token_secret});
     console.log(`reconnecting ${clientName} attempt [${connAttempts}]`);
     setTimeout(TrackStream(twitterClientPool.get(clientName), filter, connAttempts+1, clientName, supportedLanguages), RECONNECT_DELAY_MS);
}

appInsightsClient.trackEvent("Starting Twitter Stream", {outputQueue: QUEUE});

function Listen(siteDefinition){
  const trackedTerms = Array.from(new Set(siteDefinition.locations.map(item=>item.name))); 
  console.log(siteDefinition.twitterAccounts);
  CreatePooledConnections(siteDefinition.twitterAccounts);

  queueService.createQueueIfNotExists(QUEUE, (err, result, response) => {
      if (err) {
        appInsightsClient.trackException(new Error("Unable to access/create queue"));
        return;
      }

      let requestedTermsCount = 0;
      let pooledConnentionKeys = Array.from(twitterClientPool.keys());
      if((pooledConnentionKeys.length * MAX_TERMS_ALLOWED) < trackedTerms.length){
          const errMsg = `your connection pool will only allow you to track ${pooledConnentionKeys.length * MAX_TERMS_ALLOWED} of the ${trackedTerms.length} terms. Please add ${parseInt(Math.round((trackedTerms.length - (pooledConnentionKeys.length * MAX_TERMS_ALLOWED))/MAX_TERMS_ALLOWED))} more connections in the admin page.`;
          appInsightsClient.trackException(new Error(errMsg));
          console.error(errMsg);
      }

      while(requestedTermsCount <= trackedTerms.length && pooledConnentionKeys.length > 0) {
        let connectionKey = pooledConnentionKeys.pop();
        const location = siteDefinition.settings.targetBbox.join(",");
        let termsSlice = trackedTerms.slice(requestedTermsCount, requestedTermsCount + MAX_TERMS_ALLOWED < trackedTerms.length ? requestedTermsCount + MAX_TERMS_ALLOWED : trackedTerms.length);
        let filter = Object.assign({}, {locations: location}, {track: termsSlice.join(",")});
        TrackStream(twitterClientPool.get(connectionKey), filter, 0, connectionKey, siteDefinition.settings.supportedLanguages);          
        requestedTermsCount += MAX_TERMS_ALLOWED;
      }
   });
 }

function PreValidated(siteConfiguration){
  let errMsg;

  if(!QUEUE){
    errMsg = "QUEUE is undefined error";
  }else if(!FORTIS_SITE_NAME){
    errMsg = "FORTIS_SITE_NAME is undefined error";
  }else if(siteConfiguration.locations.length === 0){
    errMsg = "FORTIS_SITE_NAME was an undefined list of localities";
  }else if(!siteConfiguration.settings.targetBbox){
    errMsg = "Target Region undefined error";
  }else if(!siteConfiguration.settings.supportedLanguages){
    errMsg = "Supported language undefined error";
  }else if(siteConfiguration.settings.supportedLanguages.length === 0){
    errMsg = "empty supportedLanguages list error";
  }else if(!siteConfiguration.twitterAccounts){
    errMsg = "twitterAccounts undefined error";
  }else if(!siteConfiguration.twitterAccounts.length === 0){
    errMsg = "empty twitterAccounts list error";
  }

  return errMsg;
}

Services.getSiteDefintion(FORTIS_SITE_NAME, (error, results) =>{
        const validationError = PreValidated(results) 
        
        if(!error && !validationError){
          Listen(results);
        }else{
          const errorMsg = validationError || error;
          appInsightsClient.trackException(errorMsg);
          console.error(errorMsg);
        }
});