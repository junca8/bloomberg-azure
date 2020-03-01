using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Bloomberglp.Blpapi;
using System.Data.SqlClient;
using System.Text;
using Models;
using System.Collections.Generic;
using System.Globalization;

namespace BloombergNormalization
{
    public static class BloombergNormalization
    {
        [FunctionName("BloombergNormalization")]
        public static void Run([TimerTrigger("0 0 18 * * *")]TimerInfo myTimer, ILogger log)
        {
            var str = Environment.GetEnvironmentVariable("bloombergsql");
            var securities = new List<SecurityModel>();

            using (SqlConnection conn = new SqlConnection(str))
            {
                conn.Open();
                log.LogInformation("connection success");
                StringBuilder sb = new StringBuilder();
                sb.Append("SELECT * ");
                sb.Append("FROM [dbo].[Securities];");
                String sql = sb.ToString();
                log.LogInformation("query string creation success");

                using (SqlCommand cmd = new SqlCommand(sql, conn))
                {
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        log.LogInformation("reader success");
                        while (reader.Read())
                        {
                            var security = new SecurityModel(reader.GetInt32(0), reader.GetString(1));
                            securities.Add(security);
                        }
                    }
                }
            }

            BloombergNormalization.getBloombergData(securities, log);
        }

        private static void getBloombergData(List<SecurityModel> securities, ILogger log)
        {
            SessionOptions sessionOptions = new SessionOptions();
            sessionOptions.ServerHost = "localhost";
            sessionOptions.ServerPort = 8194;

            Session session = new Session(sessionOptions);
            if (session.Start() && session.OpenService("//blp/refdata"))
            {
                Service refDataSvc = session.GetService("//blp/refdata");
                if (refDataSvc == null)
                {
                    log.LogInformation("Cannot get service");
                }
                else
                {
                    CorrelationID requestID = new CorrelationID(1);
                    Request request = refDataSvc.CreateRequest("ReferenceDataRequest");

                    foreach (SecurityModel security in securities)
                    {
                        log.LogInformation("appending " + security.Name);
                        request.Append("securities", security.Name);
                    }

                    { //append regular fields
                        //include the following simple fields in the result
                        //request.Append("fields", "ZPX_LAST"); //the code treats a field that starts with a "Z" as a bad field
                        request.Append("fields", "PX_LAST");
                        request.Append("fields", "BID");
                        request.Append("fields", "ASK");

                        request.Append("fields", "TICKER");
                        request.Append("fields", "TRADEABLE_DT"); //hard-coded to be treated as a datetime to illustrated datetimes
                        request.Append("fields", "OPT_EXPIRE_DT"); //only stock options have this field

                        request["fields"].AppendValue("TICKER"); //This is another way to append a field
                    }

                    { //append an overridable field
                        //request a field that can be overriden and returns bulk data
                        request.Append("fields", "CHAIN_TICKERS"); //only stocks have this field 
                        Element overrides = request["overrides"];

                        //request only puts
                        Element ovrdPutCall = overrides.AppendElement();
                        ovrdPutCall.SetElement("fieldId", "CHAIN_PUT_CALL_TYPE_OVRD");
                        ovrdPutCall.SetElement("value", "P"); //accepts either "C" for calls or "P" for puts

                        //request 5 options in the result
                        Element ovrdNumStrikes = overrides.AppendElement();
                        ovrdNumStrikes.SetElement("fieldId", "CHAIN_POINTS_OVRD");
                        ovrdNumStrikes.SetElement("value", 5); //accepts a positive integer

                        //request options that expire on Dec. 20, 2014
                        Element ovrdDtExps = overrides.AppendElement();
                        ovrdDtExps.SetElement("fieldId", "CHAIN_EXP_DT_OVRD");
                        ovrdDtExps.SetElement("value", "20141220"); //accepts dates in the format yyyyMMdd (this is Dec. 20, 2014)
                    }

                    session.SendRequest(request, requestID);

                    bool continueToLoop = true;
                    while (continueToLoop)
                    {
                        Event eventObj = session.NextEvent();
                        switch (eventObj.Type)
                        {
                            case Event.EventType.RESPONSE: // final event
                                continueToLoop = false;
                                BloombergNormalization.handleResponseEvent(eventObj, securities, log);
                                break;
                            case Event.EventType.PARTIAL_RESPONSE:
                                BloombergNormalization.handleResponseEvent(eventObj, securities, log);
                                break;
                            default:
                                BloombergNormalization.handleOtherEvent(eventObj, log);
                                break;
                        }
                    }
                }
            }
            else
            {
                log.LogInformation("Cannot connect to server.  Check that the server host is \"localhost\" or \"127.0.0.1\" and that the server port is 8194.");
            }
        }

        private static void handleResponseEvent(Event eventObj, List<SecurityModel> securities, ILogger log)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("INSERT INTO [dbo].[Prices] ");
            sb.Append("(security_id, ask, bid, px_last, date_time) VALUES");

            log.LogInformation("EventType =" + eventObj.Type);
            foreach (Message message in eventObj.GetMessages())
            {
                log.LogInformation("correlationID=" + message.CorrelationID);
                log.LogInformation("messageType =" + message.MessageType);

                Element elmSecurityDataArray = message["securityData"];

                for (int valueIndex = 0; valueIndex < elmSecurityDataArray.NumValues; valueIndex++)
                {
                    Element elmSecurityData = elmSecurityDataArray.GetValueAsElement(valueIndex);

                    string security = elmSecurityData.GetElementAsString("security");
                    log.LogInformation("about to log security");
                    log.LogInformation(security);

                    bool hasFieldErrors = elmSecurityData.HasElement("fieldExceptions", true);
                    if (hasFieldErrors)
                    {
                        Element elmFieldErrors = elmSecurityData["fieldExceptions"];
                        for (int errorIndex = 0; errorIndex < elmFieldErrors.NumValues; errorIndex++)
                        {
                            Element fieldError = elmFieldErrors.GetValueAsElement(errorIndex);
                            string fieldId = fieldError.GetElementAsString("fieldId");

                            Element errorInfo = fieldError["errorInfo"];
                            string source = errorInfo.GetElementAsString("source");
                            int code = errorInfo.GetElementAsInt32("code");
                            string category = errorInfo.GetElementAsString("category");
                            string strMessage = errorInfo.GetElementAsString("message");
                            string subCategory = errorInfo.GetElementAsString("subcategory");
                            
                            log.LogInformation("\tfield error: " + security);
                            log.LogInformation(string.Format("\tfieldId = {0}", fieldId));
                            log.LogInformation(string.Format("\tsource = {0}", source));
                            log.LogInformation(string.Format("\tcode = {0}", code));
                            log.LogInformation(string.Format("\tcategory = {0}", category));
                            log.LogInformation(string.Format("\terrorMessage = {0}", strMessage));
                            log.LogInformation(string.Format("\tsubCategory = {0}", subCategory));
                        }
                    }

                    bool isSecurityError = elmSecurityData.HasElement("securityError", true);
                    if (isSecurityError)
                    {
                        Element secError = elmSecurityData["securityError"];
                        string source = secError.GetElementAsString("source");
                        int code = secError.GetElementAsInt32("code");
                        string category = secError.GetElementAsString("category");
                        string errorMessage = secError.GetElementAsString("message");
                        string subCategory = secError.GetElementAsString("subcategory");

                        log.LogInformation("\tsecurity error");
                        log.LogInformation(string.Format("\tsource = {0}", source));
                        log.LogInformation(string.Format("\tcode = {0}", code));
                        log.LogInformation(string.Format("\tcategory = {0}", category));
                        log.LogInformation(string.Format("\terrorMessage = {0}", errorMessage));
                        log.LogInformation(string.Format("\tsubCategory = {0}", subCategory));
                    }
                    else
                    {
                        Element elmFieldData = elmSecurityData["fieldData"];

                        double pxLast = elmFieldData.GetElementAsFloat64("PX_LAST");
                        double bid = elmFieldData.GetElementAsFloat64("BID");
                        double ask = elmFieldData.GetElementAsFloat64("ASK");
                        string ticker = elmFieldData.GetElementAsString("TICKER");
                        
                        log.LogInformation("\tPX_LAST = " + pxLast.ToString());
                        log.LogInformation("\tBID = " + bid.ToString());
                        log.LogInformation("\tASK = " + ask.ToString());

                        if (elmFieldData.HasElement("TRADEABLE_DT", true))
                        {
                            Datetime trDate = elmFieldData.GetElementAsDatetime("TRADEABLE_DT");
                            DateTime trDateSystem = elmFieldData.GetElementAsDatetime("TRADEABLE_DT").ToSystemDateTime(); //convenient conversion to C# DateTime object
                            log.LogInformation("\tTRADEABLE_DT = " + trDate.ToString());
                        }

                        SecurityModel bloombergSecurity = securities.Find(s => s.Name == security);
                        log.LogInformation("bloomberg security id");
                        log.LogInformation(bloombergSecurity.Id.ToString());
                        if (bloombergSecurity.Id > 0)
                        {
                            var dbDateTime = DateTime.Now;
                            sb.Append(" (" + bloombergSecurity.Id.ToString() + ", " + ask.ToString() + ", " + bid.ToString() + ", " + pxLast.ToString() + ", '" + dbDateTime.ToString() + "')");
                            if (valueIndex == elmSecurityDataArray.NumValues - 1)
                            {
                                sb.Append(";");
                            }
                            else
                            {
                                sb.Append(",");
                            }
                        }

                        //TRADEABLE_DT

                        bool excludeNullElements = true;
                        if (elmFieldData.HasElement("CHAIN_TICKERS", excludeNullElements)) //be careful, the excludeNullElements argument is false by default
                        {
                            Element chainTickers = elmFieldData["CHAIN_TICKERS"];
                            for (int chainTickerValueIndex = 0; chainTickerValueIndex < chainTickers.NumValues; chainTickerValueIndex++)
                            {
                                Element chainTicker = chainTickers.GetValueAsElement(chainTickerValueIndex);
                                string strChainTicker = chainTicker.GetElementAsString("Ticker");

                                log.LogInformation("\tCHAIN_TICKER = " + strChainTicker.ToString());
                            }
                        }
                        else
                        {
                            log.LogInformation("\tNo CHAIN_TICKER information");
                        }
                    }

                }
            }

            var str = Environment.GetEnvironmentVariable("bloombergsql");
            String sql = sb.ToString();
            log.LogInformation("here is my sql query string");
            log.LogInformation(sql);

            using (SqlConnection conn = new SqlConnection(str))
            {
                conn.Open();
                SqlCommand command = new SqlCommand(sql, conn);
                command.ExecuteNonQuery();
                log.LogInformation("all done");
            }
        }

        private static void handleOtherEvent(Event eventObj, ILogger log)
        {
            log.LogInformation("EventType=" + eventObj.Type);
            foreach (Message message in eventObj.GetMessages())
            {
                log.LogInformation("correlationID=" + message.CorrelationID);
                log.LogInformation("messageType=" + message.MessageType);
                log.LogInformation(message.ToString());
                if (Event.EventType.SESSION_STATUS == eventObj.Type && message.MessageType.Equals("SessionTerminated"))
                {
                    log.LogInformation("Terminating: " + message.MessageType);
                }
            }
        }
    }
}
