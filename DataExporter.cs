using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading;
using System.Timers;
using OTP.Common;
using OTP.Engine;
using OTP.Utility;
using OTP.Common.SFEnterpriseWSDL;
using log4net;
using Microsoft.Practices.EnterpriseLibrary.Logging;

namespace OTP.DataExporter
{
    public partial class DataExporter : ServiceBase {

        private static ILog logger = LogManager.GetLogger(typeof(DataExporter));

        System.Timers.Timer timer = new System.Timers.Timer();
        System.Timers.Timer timerDaily = new System.Timers.Timer();

        private int monitorInterval;
        private string SessionId;
        private string BaseURL;
        private bool isTest;

        private int processAccounts = 1;
        private int processCampaigns = 1;
        private int processContacts = 1;
        private int processLeads = 1;
        private int processNotes = 1;
        private int processOpportunities = 1;
        private int processOpportunityContactRoles = 1;
        private int processPartners = 1;
        private int processPeriods = 1;
        private int processTasks = 1;
        private int processAttachments = 1;
        private int continueProcessing = 1;

        private int daysToProcess = 1;

        public DataExporter() {
            InitializeComponent();
        }

        protected override void OnStart(string[] args) {
            try {

                WriteToLog("OTP.DataExporter started...", Constants.LogSeverityEnum.Information);

                System.Net.ServicePointManager.SecurityProtocol = System.Net.SecurityProtocolType.Tls12 | System.Net.SecurityProtocolType.Tls11; // comparable to modern browsers

                isTest = false;
                var testing = 0;
                try
                {
                    testing = Int32.Parse(ConfigurationManager.AppSettings["testing"].ToString());
                }
                catch
                {
                    testing = 0;
                }
                isTest = (testing == 1);

                int runInitialProcess = 0;
                try {
                    runInitialProcess = Int32.Parse(ConfigurationManager.AppSettings["runInitialProcess"].ToString());
                }
                catch { }

                int runInitialAttachmentProcess = 0;
                try
                {
                    runInitialAttachmentProcess = Int32.Parse(ConfigurationManager.AppSettings["runInitialAttachmentProcess"].ToString());
                }
                catch { }

                // get config options
                try
                {
                    processAccounts = Int32.Parse(ConfigurationManager.AppSettings["processAccounts"].ToString());
                    processCampaigns = Int32.Parse(ConfigurationManager.AppSettings["processCampaigns"].ToString());
                    processContacts = Int32.Parse(ConfigurationManager.AppSettings["processContacts"].ToString());
                    processLeads = Int32.Parse(ConfigurationManager.AppSettings["processLeads"].ToString());
                    processNotes = Int32.Parse(ConfigurationManager.AppSettings["processNotes"].ToString());
                    processOpportunities = Int32.Parse(ConfigurationManager.AppSettings["processOpportunities"].ToString());
                    processOpportunityContactRoles = Int32.Parse(ConfigurationManager.AppSettings["processOpportunityContactRoles"].ToString());
                    processPartners = Int32.Parse(ConfigurationManager.AppSettings["processPartners"].ToString());
                    processPeriods = Int32.Parse(ConfigurationManager.AppSettings["processPeriods"].ToString());
                    processTasks = Int32.Parse(ConfigurationManager.AppSettings["processTasks"].ToString());
                    processAttachments = Int32.Parse(ConfigurationManager.AppSettings["processAttachments"].ToString());
                    continueProcessing = Int32.Parse(ConfigurationManager.AppSettings["continueProcessing"].ToString());
                    daysToProcess = Int32.Parse(ConfigurationManager.AppSettings["daysToProcess"].ToString());
                }
                catch { }

                if (runInitialProcess == 1) {

                    /*
                    // run processes initially
                    System.Threading.Thread tdata = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(ProcessData));
                    tdata.Start(null);
                    */

                    string username = ConfigurationManager.AppSettings["username"].ToString();
                    string password = ConfigurationManager.AppSettings["password"].ToString();

                    SforceService binding = new SforceService();
                    binding.Timeout = 60000;
                    if (isTest)
                    {
                        //binding.Url = "https://cs16.salesforce.com/services/Soap/c/32.0/0DF40000000TVzh";
                    }
                    LoginResult lr = binding.login(username, password);

                    ProcessParams p = new ProcessParams();
                    p.binding = binding;
                    p.loginResult = lr;
                    p.startDate = DateTime.UtcNow.AddSeconds(-(daysToProcess * 86400)); // 86400 seconds in a day
                    p.endDate = DateTime.UtcNow;
                    p.useDates = true;

                    System.Threading.Thread tdata = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(ProcessUpdated));
                    tdata.Start(p);

                    System.Threading.Thread t = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(ProcessDeleted));
                    t.Start(p);

                    if (processAttachments == 1)
                    {
                        System.Threading.Thread tattach = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(ProcessAttachments));
                        tattach.Start(p);
                    }

                    if (continueProcessing == 1)
                    {
                        StartTimer();
                    }
                }
                else if (runInitialAttachmentProcess == 1)
                {
                    ProcessParams p = new ProcessParams();
                    p.getAll = true;
                    System.Threading.Thread tattach = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(ProcessAttachments));
                    tattach.Start(p);

                    // start normal process
                    //StartTimer();
                }
                else
                {
                    // start the timer to process updates and deletes incrementally
                    // **********************************************************************************
                    StartTimer();
                }

                /*
                monitorInterval = Int32.Parse(ConfigurationManager.AppSettings["interval"].ToString());

                ProcessParams p = new ProcessParams();
                p.startDate = DateTime.UtcNow.AddSeconds(-(monitorInterval * 60));
                p.endDate = DateTime.UtcNow;
                System.Threading.Thread tattach = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(ProcessAttachments));
                tattach.Start(p);
                */

                /*
                System.Threading.Thread tdata = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(ProcessData));
                tdata.Start(null);
                */
            }
            catch (Exception ex) {
                WriteToLog(ex.Message);
            }
        }

        protected override void OnStop() {
            try {
                timer.Enabled = false;
                WriteToLog("OTP.DataExporter stopped.");
            }
            catch (Exception ex) {
                WriteToLog(ex.Message);
            }
        }

        private void StartTimer() {
            try {

                var now = System.DateTime.Now;

                // get next half hour value
                var min = now.Minute;
                var hour = now.Hour;
                var add = 0;
                if (min < 30) {
                    add = (30 - min);
                }
                else {
                    add = (60 - min);
                }

                // get next midnight
                var addday = now.AddDays(1);
                var tomorrow = new System.DateTime(addday.Year, addday.Month, addday.Day, 0, 0, 0);

                // override for testing

                if (isTest)
                {
                    add = 1;
                }

                WriteToLog("OTP.DataExporter will run again in " + add.ToString() + " minutes...", Constants.LogSeverityEnum.Information);
                WriteToLog("OTP.DataExporter FULL will run again at " + tomorrow.ToString("MM/dd/yyyy hh:mm"), Constants.LogSeverityEnum.Information);

                //read from current app.config as default
                monitorInterval = Int32.Parse(ConfigurationManager.AppSettings["interval"].ToString());

                // set timer
                timer.Elapsed += new ElapsedEventHandler(timer_Elapsed);
                timer.Interval = (add * 60 * 1000);
                timer.Enabled = true;

                // set timer
                timerDaily.Elapsed += new ElapsedEventHandler(timerDaily_Elapsed);
                timerDaily.Interval = (24 * 60 * 60 * 1000);
                timerDaily.Enabled = true;
            }
            catch (Exception ex) {
                WriteToLog(ex.Message);
            }
        }
        void timer_Elapsed(object sender, ElapsedEventArgs e) {
            try {

                // Process records incrementally
                Process();

                // set timer
                timer.Elapsed -= new ElapsedEventHandler(timer_Elapsed);
                timer.Elapsed += new ElapsedEventHandler(timer_Elapsed);
                timer.Interval = (monitorInterval * 60 * 1000);
                timer.Enabled = true;

                WriteToLog("OTP.DataExporter will run again in " + monitorInterval.ToString() + " minutes...");
            }
            catch (Exception ex) {
                WriteToLog(ex.Message + ex.StackTrace);
            }
        }

        void timerDaily_Elapsed(object sender, ElapsedEventArgs e)
        {
            try
            {
                // Process all records
                //System.Threading.Thread tdata = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(ProcessData));
                //tdata.Start(null);

                string username = ConfigurationManager.AppSettings["username"].ToString();
                string password = ConfigurationManager.AppSettings["password"].ToString();

                SforceService binding = new SforceService();
                binding.Timeout = 60000;
                LoginResult lr = binding.login(username, password);

                ProcessParams p = new ProcessParams();
                p.binding = binding;
                p.loginResult = lr;
                p.startDate = DateTime.UtcNow.AddSeconds(-(daysToProcess * 86400)); // 86400 seconds in a day
                p.endDate = DateTime.UtcNow;
                p.useDates = true;

                System.Threading.Thread tdata = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(ProcessUpdated));
                tdata.Start(p);

                System.Threading.Thread t = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(ProcessDeleted));
                t.Start(p);

                if (processAttachments == 1)
                {
                    System.Threading.Thread tattach = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(ProcessAttachments));
                    tattach.Start(p);
                }

                // set timer
                timerDaily.Elapsed -= new ElapsedEventHandler(timerDaily_Elapsed);
                timerDaily.Elapsed += new ElapsedEventHandler(timerDaily_Elapsed);
                timerDaily.Interval = (24 * 60 * 60 * 1000);
                timerDaily.Enabled = true;

                WriteToLog("OTP.DataExporter FULL will run again in " + (24 * 60).ToString() + " minutes...");
            }
            catch (Exception ex)
            {
                WriteToLog(ex.Message + ex.StackTrace);
            }
        }

        private struct ProcessParams {
            public SforceService binding;
            public LoginResult loginResult;
            public DateTime startDate;
            public DateTime endDate;
            public bool useDates;
            public bool getAll;
        }

        private void Process() {
            try {

                if (isTest) Thread.Sleep(20000);

                string username = ConfigurationManager.AppSettings["username"].ToString(); //"bob.drozdowski%40gmail.com";
                string password = ConfigurationManager.AppSettings["password"].ToString(); //"Ontap123I2eCm8qmFdKeU5X0mBDRD8s2";

                SforceService binding = new SforceService();
                binding.Timeout = 60000;
                if (isTest)
                {
                    //binding.Url = "https://cs16.salesforce.com/services/Soap/c/32.0/0DF40000000TVzh";
                }
                LoginResult lr = binding.login(username, password);
                
                ProcessParams p = new ProcessParams();
                p.binding = binding;
                p.loginResult = lr;
                double interval = (monitorInterval * 60);
                if (isTest)
                {
                    interval += 86400;
                }
                p.startDate = DateTime.UtcNow.AddSeconds(-interval);
                p.endDate = DateTime.UtcNow;
                p.useDates = true;

                System.Threading.Thread tdata = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(ProcessUpdated));
                tdata.Start(p);

                System.Threading.Thread t = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(ProcessDeleted));
                t.Start(p);

                if (processAttachments == 1)
                {
                    System.Threading.Thread tattach = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(ProcessAttachments));
                    tattach.Start(p);
                }
            }
            catch (Exception ex) {
                WriteToLog(ex.Message + ex.StackTrace, Constants.LogSeverityEnum.Error);
                if (ex.Message.Contains("INVALID_LOGIN")) {
                    Utility.Email.SendMail("coreinsightgrp@gmail.com", "noreply@coreinsightgroup.com", "OTP Data Exporter Login Failed", ex.Message + ": " + ex.StackTrace);
                }
            }
        }

        private void ProcessDeleted(object stateInfo) {
            try {
                if (isTest) { Thread.Sleep(20000); }
                ProcessParams p = (ProcessParams)stateInfo;
                string[] entities = new string[] { "Account", "Attachment", "Campaign", "Contact", "Lead", "Note", "Opportunity", "Partner" };
                foreach (string s in entities) {
                    DeleteRecords(s, p);
                }
            }
            catch (Exception ex) {
                WriteToLog(ex.Message + ex.StackTrace, Constants.LogSeverityEnum.Error);
                if (ex.Message.Contains("REQUEST_LIMIT_EXCEEDED")) {
                    Utility.Email.SendMail("coreinsightgrp@gmail.com", "noreply@coreinsightgroup.com", "OTP Data Exporter Failed", ex.Message + ": " + ex.StackTrace);
                }
            }
        }
        private void DeleteRecords(string entityName, ProcessParams p) {

            WriteToLog("Processing Deletes...");

            p.binding.Url = p.loginResult.serverUrl;
            SessionHeader hdr = new SessionHeader();
            hdr.sessionId = p.loginResult.sessionId;
            p.binding.SessionHeaderValue = hdr;

            LoggingEngine.Log("OTP.DataExporter.DeleteRecords: " + entityName + ", startDate: " + p.startDate.ToString() + ", endDate: " + p.endDate.ToString(), Constants.LogSeverityEnum.Information);

            GetDeletedResult gdResult = p.binding.getDeleted(entityName, p.startDate, p.endDate);
            DeletedRecord[] deletedRecords = gdResult.deletedRecords;

            if (deletedRecords != null && deletedRecords.Length > 0) {
                for (int i = 0; i < deletedRecords.Length; i++) {

                    DeletedRecord dr = deletedRecords[i];
                    
                    switch (entityName) {

                        case "Account":
                            AccountEngine acctengine = new AccountEngine();
                            DataStructures.BooleanResponse acctbresp = acctengine.DeleteAccountEntity(dr.id);
                            if (acctbresp.Result) {
                                WriteToLog("Account " + dr.id + " successfully deleted");
                            }
                            else {
                                WriteToLog("Failed to delete Account " + dr.id);
                            }
                            break;
                        case "Attachment":
                            AttachmentEngine attengine = new AttachmentEngine();
                            DataStructures.BooleanResponse attbresp = attengine.DeleteAttachmentEntity(dr.id);
                            if (attbresp.Result) {
                                WriteToLog("Attachment " + dr.id + " successfully deleted");
                            }
                            else {
                                WriteToLog("Failed to delete Attachment " + dr.id);
                            }
                            break;
                        case "Campaign":
                            CampaignEngine cengine = new CampaignEngine();
                            DataStructures.BooleanResponse cbresp = cengine.DeleteCampaignEntity(dr.id);
                            if (cbresp.Result) {
                                WriteToLog("Campaign " + dr.id + " successfully deleted");
                            }
                            else {
                                WriteToLog("Failed to delete Campaign " + dr.id);
                            }
                            break;
                        case "Contact":
                            ContactEngine ctengine = new ContactEngine();
                            DataStructures.BooleanResponse ctbresp = ctengine.DeleteContactEntity(dr.id);
                            if (ctbresp.Result) {
                                WriteToLog("Contact " + dr.id + " successfully deleted");
                            }
                            else {
                                WriteToLog("Failed to delete Contact " + dr.id);
                            }
                            break;
                        case "Lead":
                            LeadEngine lengine = new LeadEngine();
                            DataStructures.BooleanResponse lbresp = lengine.DeleteLeadEntity(dr.id);
                            if (lbresp.Result) {
                                WriteToLog("Lead " + dr.id + " successfully deleted");
                            }
                            else {
                                WriteToLog("Failed to delete Lead " + dr.id);
                            }
                            break;
                        case "Note":
                            NoteEngine nengine = new NoteEngine();
                            DataStructures.BooleanResponse nbresp = nengine.DeleteNoteEntity(dr.id);
                            if (nbresp.Result) {
                                WriteToLog("Note " + dr.id + " successfully deleted");
                            }
                            else {
                                WriteToLog("Failed to delete Note " + dr.id);
                            }
                            break;
                        case "Opportunity":
                            OpportunityEngine engine = new OpportunityEngine();
                            DataStructures.BooleanResponse bresp = engine.DeleteOpportunityEntity(dr.id);
                            if (bresp.Result) {
                                WriteToLog("Opportunity " + dr.id + " successfully deleted");
                            }
                            else {
                                WriteToLog("Failed to delete Opportunity " + dr.id);
                            }
                            break;
                        case "Partner":
                            PartnerEngine pengine = new PartnerEngine();
                            DataStructures.BooleanResponse pbresp = pengine.DeletePartnerEntity(dr.id);
                            if (pbresp.Result) {
                                WriteToLog("Partner " + dr.id + " successfully deleted");
                            }
                            else {
                                WriteToLog("Failed to delete Partner " + dr.id);
                            }
                            break;
                        /*
                        case "Period":
                            PeriodEngine peengine = new PeriodEngine();
                            DataStructures.BooleanResponse pebresp = peengine.DeletePeriodEntity(dr.id);
                            if (pebresp.Result) {
                                WriteToLog("Period " + dr.id + " successfully deleted");
                            }
                            else {
                                WriteToLog("Failed to delete Period " + dr.id);
                            }
                            break;
                        case "Task":
                            TaskEngine tengine = new TaskEngine();
                            DataStructures.BooleanResponse tbresp = tengine.DeleteTaskEntity(dr.id);
                            if (tbresp.Result) {
                                WriteToLog("Task " + dr.id + " successfully deleted");
                            }
                            else {
                                WriteToLog("Failed to delete Task " + dr.id);
                            }
                            break;
                       */
                    }
                }
            }
            else {
                WriteToLog("No deletions of " + entityName + " records found.");
            }
        }

        private void ProcessUpdated(object stateInfo) {
            try {

                if (isTest) Thread.Sleep(20000);

                WriteToLog("Processing Updates...", Constants.LogSeverityEnum.Information);
                ProcessParams p = (ProcessParams)stateInfo;

                //string[] entities = new string[] { "Account", "Campaign", "Contact", "Lead", "Note", "Opportunity", "Partner" };

                List<string> entityList = new List<string>();
                List<string> fieldList = new List<string>();

                if (processAccounts == 1)
                {
                    entityList.Add("Account");
                    string acctfields = "Id,IsDeleted,MasterRecordId,Name,Type,ParentId,BillingStreet,BillingCity,BillingState,BillingPostalCode,BillingCountry,BillingLatitude,BillingLongitude,ShippingStreet,ShippingCity,ShippingState,ShippingPostalCode,ShippingCountry,ShippingLatitude,ShippingLongitude,Phone,Fax,Website,Industry,AnnualRevenue,NumberOfEmployees,Description,OwnerId,CreatedDate,CreatedById,LastModifiedDate,LastModifiedById,SystemModstamp,LastActivityDate,IsOTPClient__c,Number_of_Leads__c";
                    fieldList.Add(acctfields);
                }
                /*
                if (processAttachments == 1)
                {
                    entityList.Add("Attachment");
                    string attfields = "Id,IsDeleted,IsPrivate,ParentId,Name,ContentType,BodyLength,Body,OwnerId,Description,CreatedDate,CreatedById,LastModifiedDate,LastModifiedById,SystemModstamp";
                    fieldList.Add(attfields);
                }
                */
                if (processCampaigns == 1)
                {
                    entityList.Add("Campaign");
                    string cfields = "Id,IsDeleted,Name,ParentId,Type,Status,StartDate,EndDate,ExpectedRevenue,BudgetedCost,ActualCost,ExpectedResponse,NumberSent,IsActive,Description,NumberOfLeads,NumberOfConvertedLeads,NumberOfContacts,NumberOfResponses,NumberOfOpportunities,NumberOfWonOpportunities,AmountAllOpportunities,AmountWonOpportunities,OwnerId,CreatedDate,CreatedById,LastModifiedDate,LastModifiedById,SystemModstamp,LastActivityDate,Stakeholder__c,Partner__c,Campaign_News__c,OTP_AcctManager_Email__c, OTP_AcctManager_Name__c, OTP_AcctManager_Phone__c, OTP_AcctManager_Title__c,IQ001__c,IQ002__c,IQ003__c,IQ004__c,IQ005__c,IQ006__c,IQ007__c,IQ008__c,IQ009__c,IQ010__c,IQ011__c,IQ012__c,IQ013__c,IQ014__c,IQ015__c,Cover_Sheet_Text__c,Ramp_Up_Call_Scheduled__c,Ramp_Up_Call_Completed__c,Account_List_Sent_for_Parter_Approval__c,Partner_Approved_Account_List__c,Calling_Has_Begun__c,Calling_Completed__c,Gathering_Outstanding_Feedback__c,Campaign_Completed__c,List_criteria_zip_code__c,List_criteria_state__c,List_criteria_company_size__c,List_criteria_annual_revenue__c,List_criteria_date_contacted__c";
                    fieldList.Add(cfields);
                }
                if (processContacts == 1)
                {
                    entityList.Add("Contact");
                    string confields = "Id,IsDeleted,MasterRecordId,AccountId,FirstName,LastName,Salutation,Name,OtherStreet,OtherCity,OtherState,OtherPostalCode,OtherCountry,MailingStreet,MailingCity,MailingState,MailingPostalCode,MailingCountry,Phone,Fax,MobilePhone,HomePhone,OtherPhone,AssistantPhone,ReportsToId,Email,Title,Department,AssistantName,LeadSource,Birthdate,Description,OwnerId,CreatedDate,CreatedById,LastModifiedDate,LastModifiedById,SystemModstamp,LastActivityDate,LastCURequestDate,LastCUUpdateDate,EmailBouncedReason,EmailBouncedDate,Receives_Email_Notifications__c,Is_Primary_Email_Recipient__c";
                    fieldList.Add(confields);
                }
                if (processLeads == 1)
                {
                    entityList.Add("Lead");
                    string lfields = "Id,IsDeleted,MasterRecordId,Salutation,FirstName,LastName,Title,Company,Street,City,State,PostalCode,Country,Phone,Email,Website,Description,LeadSource,Status,Industry,Rating,AnnualRevenue,NumberOfEmployees,OwnerId,IsConverted,ConvertedDate,ConvertedAccountId,ConvertedContactId,ConvertedOpportunityId,IsUnreadByOwner,CreatedDate,CreatedById,LastModifiedDate,LastModifiedById,SystemModstamp,LastActivityDate,EmailBouncedReason,EmailBouncedDate";
                    fieldList.Add(lfields);
                }
                if (processNotes == 1)
                {
                    entityList.Add("Note");
                    string nfields = "Id,IsDeleted,ParentId,Title,IsPrivate,Body,OwnerId,CreatedDate,CreatedById,LastModifiedDate,LastModifiedById,SystemModstamp";
                    fieldList.Add(nfields);
                }
                if (processOpportunities == 1)
                {
                    entityList.Add("Opportunity");
                    string ofields = "Id,IsDeleted,AccountId,Name,Description,StageName,Amount,Probability,CloseDate,Type,NextStep,LeadSource,IsClosed,IsWon,ForecastCategory,ForecastCategoryName,CampaignId,HasOpportunityLineItem,Pricebook2Id,FiscalQuarter,FiscalYear,Fiscal,CreatedDate,CreatedById,LastModifiedDate,LastModifiedById,SystemModstamp,LastActivityDate,Meeting_Date_Time__c,IsOTP_Approved__c,Registered_Deal_Num__c,IQ001__c,IQ002__c,IQ003__c,IQ004__c,IQ005__c,IQ006__c,IQ007__c,IQ008__c,IQ009__c,IQ010__c,IQ011__c,IQ012__c,IQ013__c,IQ014__c,IQ015__c,Cover_Sheet_Text__c";
                    fieldList.Add(ofields);
                }
                if (processOpportunityContactRoles == 1)
                {
                    entityList.Add("OpportunityContactRole");
                    string ocrfields = "ContactId,OpportunityId,IsDeleted,IsPrimary,Role,CreatedDate,CreatedById,LastModifiedDate,LastModifiedById,SystemModstamp";
                    fieldList.Add(ocrfields);
                }
                if (processPartners == 1)
                {
                    entityList.Add("Partner");
                    string pfields = "Id,IsDeleted,OpportunityId,AccountFromId,AccountToId,Role,IsPrimary,ReversePartnerId,CreatedDate,CreatedById,LastModifiedDate,LastModifiedById,SystemModstamp";
                    fieldList.Add(pfields);
                }
                if (processPeriods == 1)
                {
                    entityList.Add("Period");
                    string perfields = "Id,FiscalYearSettingsId,Type,StartDate,EndDate,QuarterLabel,IsForecastPeriod,PeriodLabel,Number,SystemModstamp";
                    fieldList.Add(perfields);
                }
                if (processTasks == 1)
                {
                    entityList.Add("Task");
                    string tfields = "Id,IsClosed,AccountId,ActivityDate,Description,Status,Subject,WhatId,WhoId,CreatedDate,CreatedById,LastModifiedDate,LastModifiedById,SystemModstamp";
                    fieldList.Add(tfields);
                }

                int idx = 0;
                foreach (string s in entityList) {
                    UpdateRecords(s, fieldList[idx], p);
                    idx++;
                }
            }
            catch (Exception ex) {
                WriteToLog(ex.Message + ex.StackTrace, Constants.LogSeverityEnum.Error);
                if (ex.Message.Contains("REQUEST_LIMIT_EXCEEDED")) {
                    Utility.Email.SendMail("coreinsightgrp@gmail.com", "noreply@coreinsightgroup.com", "OTP Data Exporter Failed", ex.Message + ": " + ex.StackTrace);
                }
            }
        }
        private void UpdateRecords(string entityName, string fields, ProcessParams p) {

            try {

                p.binding.Url = p.loginResult.serverUrl;
                SessionHeader hdr = new SessionHeader();
                hdr.sessionId = p.loginResult.sessionId;
                p.binding.SessionHeaderValue = hdr;

                WriteToLog("OTP.DataExporter.UpdateRecords: " + entityName + ", startDate: " + p.startDate.ToString() + ", endDate: " + p.endDate.ToString(), Constants.LogSeverityEnum.Information);

                GetUpdatedResult result = p.binding.getUpdated(entityName, p.startDate, p.endDate);
                string[] ids = result.ids;

                if (ids != null && ids.Length > 0) {

                    // Salesforce retrieve only allows 2000 records per retrieve call
                    int max = 2000;
                    int take = max;
                    int index = 0;

                    while (ids.Length > (max * index))
                    {
                        ids = ids.Skip((max * index)).Take(max).ToArray();
                        index++;

                        sObject[] sObjects = p.binding.retrieve(fields, entityName, ids);
                        if (sObjects != null)
                        {

                            for (int idx = 0; idx < sObjects.Length; idx++)
                            {

                                try
                                {
                                    switch (entityName)
                                    {

                                        case "Account":
                                            // Cast the SObject into an Account object
                                            Account acct = (Account)sObjects[idx];
                                            if (acct != null)
                                            {
                                                Common.Entities.AccountEntity acctentity = Common.Entities.AccountEntity.NewAccountEntity();
                                                acctentity.Merge(acct);
                                                AccountEngine acctengine = new AccountEngine();
                                                DataStructures.BooleanResponse bresp = acctengine.SetAccountEntity(acctentity);
                                                if (bresp.Result)
                                                {
                                                    WriteToLog("Account " + acct.Id + " successfully updated");
                                                }
                                                else
                                                {
                                                    WriteToLog("Failed to add or update Account " + acct.Id + "; ERROR: " + bresp.Error.Message);
                                                }
                                            }
                                            break;
                                        case "Attachment":
                                            /*
                                            // Cast the SObject into an Attachment object
                                            Attachment attachment = (Attachment)sObjects[idx];
                                            if (attachment != null) {
                                                Common.Entities.AttachmentEntity attentity = Common.Entities.AttachmentEntity.NewAttachmentEntity();
                                                attentity.Merge(attachment);

                                                // remove and ID3 tags in the mp3 file
                                                if (attentity.Body.StartsWith("ID3")) {
                                                    byte[] a = System.Convert.FromBase64String(attentity.Body);
                                                    //System.IO.MemoryStream stream = new System.IO.MemoryStream(a);
                                                    //byte[] a = System.IO.File.ReadAllBytes("X.mp3");
                                                    int x = 0;
                                                    int b = 3;
                                                    for (int i = 6; i <= 9; i++) {
                                                        x += (a[i] << (b * 7));
                                                        b--;
                                                    }
                                                    byte[] r = new byte[a.Length - x];
                                                    for (int i = x; i < a.Length; i++) {
                                                        r[i - x] = a[i];
                                                    }
                                                    //System.IO.File.WriteAllBytes("X.mp3", r);
                                                    attentity.Body = Encoding.Unicode.GetString(r);
                                                }

                                                AttachmentEngine attengine = new AttachmentEngine();
                                                DataStructures.BooleanResponse bresp = attengine.SetAttachmentEntity(attentity);
                                                if (bresp.Result) {
                                                    WriteToLog("Attachment " + attachment.Id + " successfully updated");
                                                }
                                                else {
                                                    WriteToLog("Failed to add or update Attachment " + attachment.Id + "; ERROR: " + bresp.Error.Message);
                                                }
                                            }
                                            */
                                            WriteToLog("Found Attachment. UpdateRecords not processing Attachments", Constants.LogSeverityEnum.Information);
                                            break;
                                        case "Campaign":
                                            // Cast the SObject into an Campaign object
                                            Campaign c = (Campaign)sObjects[idx];
                                            if (c != null)
                                            {
                                                Common.Entities.CampaignEntity centity = Common.Entities.CampaignEntity.NewCampaignEntity();
                                                centity.Merge(c);

                                                if (!string.IsNullOrEmpty(centity.Cover_Sheet_Text__c))
                                                {
                                                    StringBuilder sbcoversheet = new StringBuilder();
                                                    List<string> coversheet = centity.Cover_Sheet_Text__c.Split(new string[] { "\n" }, StringSplitOptions.RemoveEmptyEntries).ToList<string>();
                                                    if (coversheet != null)
                                                    {
                                                        foreach (string n in coversheet)
                                                        {
                                                            sbcoversheet.Append("<div>");
                                                            sbcoversheet.Append(n);
                                                            sbcoversheet.Append("</div>");
                                                            sbcoversheet.Append("<br />");
                                                        }

                                                        centity.Cover_Sheet_Text__c = sbcoversheet.ToString();
                                                    }
                                                }

                                                CampaignEngine cengine = new CampaignEngine();
                                                DataStructures.BooleanResponse bresp = cengine.SetCampaignEntity(centity);
                                                if (bresp.Result)
                                                {
                                                    WriteToLog("Campaign " + c.Id + " successfully updated");
                                                }
                                                else
                                                {
                                                    WriteToLog("Failed to add or update Campaign " + c.Id + "; ERROR: " + bresp.Error.Message);
                                                }
                                            }
                                            break;
                                        case "Contact":
                                            // Cast the SObject into an Contact object
                                            Contact contact = (Contact)sObjects[idx];
                                            if (contact != null)
                                            {
                                                Common.Entities.ContactEntity contactentity = Common.Entities.ContactEntity.NewContactEntity();
                                                contactentity.Merge(contact);
                                                ContactEngine contactengine = new ContactEngine();
                                                DataStructures.BooleanResponse bresp = contactengine.SetContactEntity(contactentity);
                                                if (bresp.Result)
                                                {
                                                    WriteToLog("Contact " + contact.Id + " successfully updated");
                                                }
                                                else
                                                {
                                                    WriteToLog("Failed to add or update Contact " + contact.Id + "; ERROR: " + bresp.Error.Message);
                                                }
                                            }
                                            break;
                                        case "Lead":
                                            // Cast the SObject into an Lead object
                                            Lead lead = (Lead)sObjects[idx];
                                            if (lead != null)
                                            {
                                                Common.Entities.LeadEntity lentity = Common.Entities.LeadEntity.NewLeadEntity();
                                                lentity.Merge(lead);
                                                LeadEngine lengine = new LeadEngine();
                                                DataStructures.BooleanResponse bresp = lengine.SetLeadEntity(lentity);
                                                if (bresp.Result)
                                                {
                                                    WriteToLog("Lead " + lead.Id + " successfully updated");
                                                }
                                                else
                                                {
                                                    WriteToLog("Failed to add or update Lead " + lead.Id + "; ERROR: " + bresp.Error.Message);
                                                }
                                            }
                                            break;
                                        case "Note":
                                            // Cast the SObject into a Note object
                                            Note note = (Note)sObjects[idx];
                                            if (note != null)
                                            {
                                                Common.Entities.NoteEntity nentity = Common.Entities.NoteEntity.NewNoteEntity();
                                                nentity.Merge(note);

                                                StringBuilder sbnotes = new StringBuilder();
                                                List<string> notes = note.Body.Split(new string[] { "\n" }, StringSplitOptions.RemoveEmptyEntries).ToList<string>();
                                                if (notes != null)
                                                {
                                                    foreach (string n in notes)
                                                    {
                                                        sbnotes.Append("<div>");
                                                        sbnotes.Append(n);
                                                        sbnotes.Append("</div>");
                                                        sbnotes.Append("<br />");
                                                    }

                                                    nentity.Body = sbnotes.ToString();
                                                }

                                                NoteEngine nengine = new NoteEngine();
                                                DataStructures.BooleanResponse bresp = nengine.SetNoteEntity(nentity);
                                                if (bresp.Result)
                                                {
                                                    WriteToLog("Note " + note.Id + " successfully updated");
                                                }
                                                else
                                                {
                                                    WriteToLog("Failed to add or update Note " + note.Id + "; ERROR: " + bresp.Error.Message);
                                                }
                                            }
                                            break;
                                        case "Opportunity":
                                            // Cast the SObject into an Opportunity object
                                            Opportunity opp = (Opportunity)sObjects[idx];
                                            if (opp != null)
                                            {
                                                WriteToLog("Opportunity.Id: " + opp.Id + ", Opportunity.IsOTP_Approved__c: " + opp.IsOTP_Approved__c + ", Opportunity.Meeting_Date_Time__c: " + opp.Meeting_Date_Time__c, "DataExporter.UpdateRecord");

                                                OpportunityEngine oengine = new OpportunityEngine();

                                                Common.Entities.OpportunityEntity oppentity = Common.Entities.OpportunityEntity.NewOpportunityEntity();
                                                oppentity.Merge(opp);

                                                bool isNew = false;
                                                bool hasChanged = false;
                                                DataStructures.OpportunityEntityResponse oentityresponse = oengine.GetOpportunityEntity(opp.Id);
                                                if (oentityresponse.Result) {
                                                    if (oentityresponse.Entity != null) {
                                                        isNew = (!oentityresponse.Entity.IsOTP_Approved__c.Equals("True"));
                                                        hasChanged = oentityresponse.Entity.HasChanged(oppentity);
                                                    }
                                                    else {
                                                        isNew = true;
                                                    }
                                                }
                                                WriteToLog("Opportunity " + opp.Id + " isNew=" + isNew.ToString() + ", hasChanged=" + hasChanged.ToString(), "DataExporter.UpdateRecord");

                                                DataStructures.BooleanResponse bresp = oengine.SetOpportunityEntity(oppentity);
                                                if (bresp.Result)
                                                {
                                                    WriteToLog("Opportunity " + opp.Id + " successfully added/updated", "DataExporter.UpdateRecord");

                                                    if (((isNew) || (hasChanged)) && (oppentity.IsOTP_Approved__c.Equals("True"))) {
                                                        Common.Entities.NotificationEntity notification = Common.Entities.NotificationEntity.NewNotificationEntity();
                                                        notification.Method = "Email";
                                                        notification.Type = (isNew ? "NewOpportunity" : "UpdatedOpportunity");
                                                        notification.Data = oppentity.Id;
                                                        NotificationEngine nengine = new NotificationEngine();
                                                        DataStructures.BooleanResponse bresponse = nengine.SetNotificationEntity(notification);
                                                        if (!bresponse.Result) {
                                                            WriteToLog("OTP.NotificationService SetNotificationEntity failed for " + notification.Type + ": " + oppentity.Id + "; ERROR: " + bresponse.Error.Message, "DataExporter.UpdateRecord");
                                                        }
                                                        else
                                                        {
                                                            WriteToLog("SetNotificationEntity success: " + notification.Type + ", Opportunity.Id = " + oppentity.Id, "DataExporter.UpdateRecord");
                                                        }
                                                    }
                                                }
                                                else
                                                {
                                                    WriteToLog("Failed to add/update Opportunity " + opp.Id + "; ERROR: " + bresp.Error.Message, "DataExporter.UpdateRecord");
                                                }
                                            }
                                            break;
                                        case "OpportunityContactRole":
                                            // Cast the SObject into an OpportunityContactRole object
                                            OpportunityContactRole oppcontactrole = (OpportunityContactRole)sObjects[idx];
                                            if (oppcontactrole != null)
                                            {
                                                Common.Entities.OpportunityContactRoleEntity ocrentity = Common.Entities.OpportunityContactRoleEntity.NewOpportunityContactRoleEntity();
                                                ocrentity.Merge(oppcontactrole);
                                                OpportunityEngine pengine = new OpportunityEngine();
                                                DataStructures.BooleanResponse bresp = pengine.SetOpportunityContactRole(ocrentity);
                                                if (bresp.Result)
                                                {
                                                    WriteToLog("OpportunityContactRole " + oppcontactrole.Id + " successfully updated");
                                                }
                                                else
                                                {
                                                    WriteToLog("Failed to add or update OpportunityContactRole " + oppcontactrole.Id + "; ERROR: " + bresp.Error.Message);
                                                }
                                            }
                                            break;
                                        case "Partner":
                                            // Cast the SObject into a Partner object
                                            Partner partner = (Partner)sObjects[idx];
                                            if (partner != null)
                                            {
                                                Common.Entities.PartnerEntity pentity = Common.Entities.PartnerEntity.NewPartnerEntity();
                                                pentity.Merge(partner);
                                                PartnerEngine pengine = new PartnerEngine();
                                                DataStructures.BooleanResponse bresp = pengine.SetPartnerEntity(pentity);
                                                if (bresp.Result)
                                                {
                                                    WriteToLog("Partner " + partner.Id + " successfully updated");
                                                }
                                                else
                                                {
                                                    WriteToLog("Failed to add or update Partner " + partner.Id + "; ERROR: " + bresp.Error.Message);
                                                }
                                            }
                                            break;
                                        case "Period":
                                            // Cast the SObject into a Period object
                                            Period period = (Period)sObjects[idx];
                                            if (period != null)
                                            {
                                                Common.Entities.PeriodEntity periodentity = Common.Entities.PeriodEntity.NewPeriodEntity();
                                                periodentity.Merge(period);
                                                PeriodEngine periodengine = new PeriodEngine();
                                                DataStructures.BooleanResponse bresp = periodengine.SetPeriodEntity(periodentity);
                                                if (bresp.Result)
                                                {
                                                    WriteToLog("Period " + period.Id + " successfully updated");
                                                }
                                                else
                                                {
                                                    WriteToLog("Failed to add or update Period " + period.Id + "; ERROR: " + bresp.Error.Message);
                                                }
                                            }
                                            break;
                                        case "Task":
                                            // Cast the SObject into a Task object
                                            Task task = (Task)sObjects[idx];
                                            if (task != null)
                                            {
                                                Common.Entities.TaskEntity taskentity = Common.Entities.TaskEntity.NewTaskEntity();
                                                taskentity.Merge(task);
                                                TaskEngine taskengine = new TaskEngine();
                                                DataStructures.BooleanResponse bresp = taskengine.SetTaskEntity(taskentity);
                                                if (bresp.Result)
                                                {
                                                    WriteToLog("Task " + task.Id + " successfully updated");
                                                }
                                                else
                                                {
                                                    WriteToLog("Failed to add or update Task " + task.Id + "; ERROR: " + bresp.Error.Message);
                                                }
                                            }
                                            break;
                                    }
                                }
                                catch (Exception ex)
                                {
                                    WriteToLog("UpdateRecords looping error: " + ex.Message + ex.StackTrace, Constants.LogSeverityEnum.Error);
                                }
                            }
                        }
                    }
                }
                else {
                    WriteToLog("No adds/updates of " + entityName + " records found.");
                }
            }
            catch (Exception ex) {
                WriteToLog("UpdateRecords error: " + ex.Message + ex.StackTrace, Constants.LogSeverityEnum.Error);
            }
        }

        private void ProcessData(object stateInfo) {

            try {

                if (isTest) Thread.Sleep(20000);

                // Login using SOAP API
                // curl --insecure https://login.salesforce.com/services/Soap/u/29.0 -H "Content-Type: text/xml; charset=UTF-8" -H "SOAPAction: login" -d @C:\code\OnTapPipeline\data\login.txt

                string username = ConfigurationManager.AppSettings["username"].ToString(); //"bob.drozdowski%40gmail.com";
                string password = ConfigurationManager.AppSettings["password"].ToString(); //"Ontap123I2eCm8qmFdKeU5X0mBDRD8s2";

                SforceService partnerApi = new SforceService();
                LoginResult loginResult = partnerApi.login(username, password);

                partnerApi.SessionHeaderValue = new SessionHeader();
                partnerApi.SessionHeaderValue.sessionId = loginResult.sessionId;
                partnerApi.Url = loginResult.serverUrl;

                /*
                WriteToLog("sessionId: " + partnerApi.SessionHeaderValue.sessionId);
                WriteToLog("serverUrl: " + partnerApi.Url);
                */

                /*
                ProcessParams p = (ProcessParams)stateInfo;

                // parse the URL
                Uri uri = new Uri(p.binding.Url);
                string instance = uri.Authority;
                string baseURL = "https://" + instance;
                this.SessionId = p.loginResult.sessionId;
                this.BaseURL = baseURL;
                */

                // parse the URL
                Uri uri = new Uri(partnerApi.Url);
                string instance = uri.Authority;
                string baseURL = "https://" + instance;
                this.SessionId = partnerApi.SessionHeaderValue.sessionId;
                this.BaseURL = baseURL;

                // process Account data
                // **********************************************************************************
                string type = "Account";
                string sql = "Select Id,IsDeleted,MasterRecordId,Name,Type,ParentId,BillingStreet,BillingCity,BillingState,BillingPostalCode,BillingCountry,BillingLatitude,BillingLongitude,ShippingStreet,ShippingCity,ShippingState,ShippingPostalCode,ShippingCountry,ShippingLatitude,ShippingLongitude,Phone,Fax,Website,Industry,AnnualRevenue,NumberOfEmployees,Description,OwnerId,CreatedDate,CreatedById,LastModifiedDate,LastModifiedById,SystemModstamp,LastActivityDate,IsOTPClient__c,Number_of_Leads__c From Account";
                if (processAccounts == 1) ProcessJob(type, sql);

                // process Attachment data
                // **********************************************************************************
                //type = "Attachment";
                //sql = "Select Id,IsDeleted,IsPrivate,ParentId,Name,ContentType,BodyLength,Body,OwnerId,Description,CreatedDate,CreatedById,LastModifiedDate,LastModifiedById,SystemModstamp From Attachment";
                //ProcessJob(type, sql);

                // process Campaign data
                // **********************************************************************************
                type = "Campaign";
                sql = "Select Id,IsDeleted,Name,ParentId,Type,Status,StartDate,EndDate,ExpectedRevenue,BudgetedCost,ActualCost,ExpectedResponse,NumberSent,IsActive,Description,NumberOfLeads,NumberOfConvertedLeads,NumberOfContacts,NumberOfResponses,NumberOfOpportunities,NumberOfWonOpportunities,AmountAllOpportunities,AmountWonOpportunities,OwnerId,CreatedDate,CreatedById,LastModifiedDate,LastModifiedById,SystemModstamp,LastActivityDate,Stakeholder__c,Partner__c,Campaign_News__c,OTP_AcctManager_Email__c, OTP_AcctManager_Name__c, OTP_AcctManager_Phone__c, OTP_AcctManager_Title__c,IQ001__c,IQ002__c,IQ003__c,IQ004__c,IQ005__c,IQ006__c,IQ007__c,IQ008__c,IQ009__c,IQ010__c,IQ011__c,IQ012__c,IQ013__c,IQ014__c,IQ015__c,Cover_Sheet_Text__c,Ramp_Up_Call_Scheduled__c,Ramp_Up_Call_Completed__c,Account_List_Sent_for_Parter_Approval__c,Partner_Approved_Account_List__c,Calling_Has_Begun__c,Calling_Completed__c,Gathering_Outstanding_Feedback__c,Campaign_Completed__c,List_criteria_zip_code__c,List_criteria_state__c,List_criteria_company_size__c,List_criteria_annual_revenue__c,List_criteria_date_contacted__c From Campaign";
                if (processCampaigns == 1) ProcessJob(type, sql);

                // process Contact data
                // **********************************************************************************
                type = "Contact";
                sql = "Select Id,IsDeleted,MasterRecordId,AccountId,FirstName,LastName,Salutation,Name,OtherStreet,OtherCity,OtherState,OtherPostalCode,OtherCountry,MailingStreet,MailingCity,MailingState,MailingPostalCode,MailingCountry,Phone,Fax,MobilePhone,HomePhone,OtherPhone,AssistantPhone,ReportsToId,Email,Title,Department,AssistantName,LeadSource,Birthdate,Description,OwnerId,CreatedDate,CreatedById,LastModifiedDate,LastModifiedById,SystemModstamp,LastActivityDate,LastCURequestDate,LastCUUpdateDate,EmailBouncedReason,EmailBouncedDate,Receives_Email_Notifications__c,Is_Primary_Email_Recipient__c From Contact";
                if (processContacts == 1) ProcessJob(type, sql);

                // process Lead data
                // **********************************************************************************
                type = "Lead";
                sql = "Select Id,IsDeleted,MasterRecordId,Salutation,FirstName,LastName,Title,Company,Street,City,State,PostalCode,Country,Phone,Email,Website,Description,LeadSource,Status,Industry,Rating,AnnualRevenue,NumberOfEmployees,OwnerId,IsConverted,ConvertedDate,ConvertedAccountId,ConvertedContactId,ConvertedOpportunityId,IsUnreadByOwner,CreatedDate,CreatedById,LastModifiedDate,LastModifiedById,SystemModstamp,LastActivityDate,EmailBouncedReason,EmailBouncedDate From Lead";
                if (processLeads == 1) ProcessJob(type, sql);

                // process Note data
                // **********************************************************************************
                type = "Note";
                sql = "Select Id,IsDeleted,ParentId,Title,IsPrivate,Body,OwnerId,CreatedDate,CreatedById,LastModifiedDate,LastModifiedById,SystemModstamp From Note";
                if (processNotes == 1) ProcessJob(type, sql);

                // process Opportunity data
                // **********************************************************************************
                type = "Opportunity";
                sql = "Select Id,IsDeleted,AccountId,Name,Description,StageName,Amount,Probability,CloseDate,Type,NextStep,LeadSource,IsClosed,IsWon,ForecastCategory,ForecastCategoryName,CampaignId,HasOpportunityLineItem,Pricebook2Id,FiscalQuarter,FiscalYear,Fiscal,CreatedDate,CreatedById,LastModifiedDate,LastModifiedById,SystemModstamp,LastActivityDate,Meeting_Date_Time__c,IsOTP_Approved__c,Registered_Deal_Num__c,IQ001__c,IQ002__c,IQ003__c,IQ004__c,IQ005__c,IQ006__c,IQ007__c,IQ008__c,IQ009__c,IQ010__c,IQ011__c,IQ012__c,IQ013__c,IQ014__c,IQ015__c,Cover_Sheet_Text__c From Opportunity";
                if (processOpportunities == 1) ProcessJob(type, sql);

                // process OpportunityContactRole data
                // **********************************************************************************
                type = "OpportunityContactRole";
                sql = "ContactId,OpportunityId,IsDeleted,IsPrimary,Role,CreatedDate,CreatedById,LastModifiedDate,LastModifiedById,SystemModstamp From OpportunityContactRole";
                if (processOpportunityContactRoles == 1) ProcessJob(type, sql);

                // process Partner data
                // **********************************************************************************
                type = "Partner";
                sql = "Select Id,IsDeleted,OpportunityId,AccountFromId,AccountToId,Role,IsPrimary,ReversePartnerId,CreatedDate,CreatedById,LastModifiedDate,LastModifiedById,SystemModstamp From Partner";
                if (processPartners == 1) ProcessJob(type, sql);

                // process Period data
                // **********************************************************************************
                type = "Period";
                sql = "Select Id,FiscalYearSettingsId,Type,StartDate,EndDate,QuarterLabel,IsForecastPeriod,PeriodLabel,Number,SystemModstamp From Period";
                if (processPeriods == 1) ProcessJob(type, sql);

                // process Task data
                // **********************************************************************************
                type = "Task";
                sql = "Select Id,IsClosed,AccountId,ActivityDate,Description,Status,Subject,WhatId,WhoId,CreatedDate,CreatedById,LastModifiedDate,LastModifiedById,SystemModstamp From Task";
                if (processTasks == 1) ProcessJob(type, sql);

                // start the timer to process updates and deletes incrementally
                // **********************************************************************************
                if (continueProcessing == 1) StartTimer();
            }
            catch (Exception ex) {
                WriteToLog(ex.Message + ex.StackTrace, Constants.LogSeverityEnum.Error);
                if (ex.Message.Contains("REQUEST_LIMIT_EXCEEDED")) {
                    Utility.Email.SendMail("coreinsightgrp@gmail.com", "noreply@coreinsightgroup.com", "OTP Data Exporter Failed", ex.Message + ": " + ex.StackTrace);
                }
            }
        }

        private void ProcessAttachments(object stateInfo)
        {
            try
            {
                if (isTest) Thread.Sleep(20000);

                ProcessParams p = (ProcessParams)stateInfo;

                WriteToLog("OTP.DataExporter.ProcessAttachments: startDate: " + p.startDate.ToString() + ", endDate: " + p.endDate.ToString(), Constants.LogSeverityEnum.Information);

                // Login using SOAP API
                // curl --insecure https://login.salesforce.com/services/Soap/u/29.0 -H "Content-Type: text/xml; charset=UTF-8" -H "SOAPAction: login" -d @C:\code\OnTapPipeline\data\login.txt

                string username = ConfigurationManager.AppSettings["username"].ToString(); //"bob.drozdowski%40gmail.com";
                string password = ConfigurationManager.AppSettings["password"].ToString(); //"Ontap123I2eCm8qmFdKeU5X0mBDRD8s2";

                SforceService partnerApi = new SforceService();
                LoginResult loginResult = partnerApi.login(username, password);

                partnerApi.SessionHeaderValue = new SessionHeader();
                partnerApi.SessionHeaderValue.sessionId = loginResult.sessionId;
                partnerApi.Url = loginResult.serverUrl;

                // parse the URL
                Uri uri = new Uri(partnerApi.Url);
                string instance = uri.Authority;
                string baseURL = "https://" + instance;
                this.SessionId = partnerApi.SessionHeaderValue.sessionId;
                this.BaseURL = baseURL;

                // process Attachment data
                // **********************************************************************************
                if (p.getAll)
                {
                    // get Attachments in chunks to avoid OutOfMemory error
                    DateTime initDate = new DateTime(2014, 1, 1);
                    DateTime dstart = DateTime.UtcNow.AddDays(-1);
                    DateTime dend = DateTime.UtcNow;
                    while (dstart >= initDate)
                    {
                        string type = "Attachment";
                        string sql = "Select Id,IsDeleted,IsPrivate,ParentId,Name,ContentType,BodyLength,Body,OwnerId,Description,CreatedDate,CreatedById,LastModifiedDate,LastModifiedById,SystemModstamp From Attachment";
                        sql += " WHERE SystemModstamp >= " + dstart.ToString("o") + " and SystemModstamp <= " + dend.ToString("o");

                        ProcessJob(type, sql);

                        dend = dstart;
                        dstart = dstart.AddDays(-1);
                    }
                }
                else
                {
                    string startDate = p.startDate.ToString("o");
                    string endDate = p.endDate.ToString("o");
                    string type = "Attachment";
                    string sql = "Select Id,IsDeleted,IsPrivate,ParentId,Name,ContentType,BodyLength,Body,OwnerId,Description,CreatedDate,CreatedById,LastModifiedDate,LastModifiedById,SystemModstamp From Attachment";
                    if (p.useDates)
                    {
                        sql += " WHERE SystemModstamp >= " + startDate + " and SystemModstamp <= " + endDate;
                    }
                    ProcessJob(type, sql);
                }
            }
            catch (Exception ex)
            {
                WriteToLog(ex.Message + ex.StackTrace, Constants.LogSeverityEnum.Error);
                if (ex.Message.Contains("REQUEST_LIMIT_EXCEEDED"))
                {
                    Utility.Email.SendMail("coreinsightgrp@gmail.com", "noreply@coreinsightgroup.com", "OTP Data Exporter Failed", ex.Message + ": " + ex.StackTrace);
                }
            }
        }

        public void ProcessJob(string type, string sql) {

            try {
                WriteToLog("Starting CreateJob for type: " + type + "...");
                var jobid = CreateJob("query", type, "XML");
                WriteToLog("CreateJob for type: " + type + ": " + jobid);

                WriteToLog("Starting CreateQueryBatch for type: " + type + "...");
                var batchid = CreateQueryBatch(jobid, sql);
                WriteToLog("CreateQueryBatch for type: " + type + ": " + batchid);

                WriteToLog("Starting CheckBatchStatus for type: " + type + "...");
                var status = CheckBatchStatus(jobid, batchid);
                WriteToLog("CheckBatchStatus for type: " + type + ": " + status);

                bool continueBatchProcessing = true;
                int max = 10;
                int count = 0;

                if (status.Equals("Failed"))
                {
                    continueBatchProcessing = false;
                    WriteToLog("CheckBatchStatus failed for " + type + "...");
                }
                else
                {
                    while (status != "Completed")
                    {

                        Thread.Sleep(20000);

                        WriteToLog("Starting CheckBatchStatus for type: " + type + "...");
                        status = CheckBatchStatus(jobid, batchid);
                        count++;

                        if (status.Equals("400Error"))
                        {
                            //status = "Completed";
                            WriteToLog("CheckBatchStatus for type: " + type + " , jobid: " + jobid + ", batchid: " + batchid + " failed.");
                        }
                        if (count >= max)
                        {
                            status = "Completed";
                            continueBatchProcessing = false;
                            WriteToLog("Reached max attempts on CheckBatchStatus for type: " + type + " , jobid: " + jobid + ", batchid: " + batchid + ". Skipping Batch");
                        }
                    }
                }

                if (continueBatchProcessing) {

                    WriteToLog("Starting GetBatchResultsLink for type: " + type + "...");
                    var link = GetBatchResultsLink(jobid, batchid);
                    WriteToLog("GetBatchResultsLink: " + link);

                    WriteToLog("Starting GetBatchResults for type: " + type + "...");
                    var results = GetBatchResults(jobid, batchid, link, type);
                    WriteToLog("GetBatchResults returned");
                }

                WriteToLog("Starting CloseJob for type: " + type + "...");
                var close = CloseJob(jobid);
                WriteToLog("CloseJob: " + close);
            }
            catch (Exception ex) {
                if (ex.Message.Contains("REQUEST_LIMIT_EXCEEDED")) {
                    Utility.Email.SendMail("support@coreinsightgroup.com", "vps@coreinsightgroup.com", "OTP Data Exporter Failed", ex.Message + ": " + ex.StackTrace);
                }
                WriteToLog(ex.Message + ex.StackTrace, Constants.LogSeverityEnum.Error);
            }
        }

        public string CreateJob(string operation, string sobject, string type) {

            string retval = string.Empty;

            try {

                string createJobUrl = this.BaseURL + "/services/async/29.0/job";
                string contentType = "application/xml; charset=UTF-8";

                // add token to header
                List<OTP.Utility.HttpHeader> headers = new List<HttpHeader>();
                HttpHeader header = new HttpHeader();
                header.Name = "X-SFDC-Session";
                header.Value = this.SessionId;
                headers.Add(header);

                // create POST body
                string postData = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
                postData += "<jobInfo xmlns=\"http://www.force.com/2009/06/asyncapi/dataload\">";
                postData += "<operation>" + operation + "</operation>";
                postData += "<object>" + sobject + "</object>";
                postData += "<contentType>" + type + "</contentType>";
                postData += "</jobInfo>";

                // send POST request
                string response = Http.SendHttpPost(createJobUrl, postData, headers, contentType);

                // process XML response
                var doc = new System.Xml.XmlDocument();
                doc.LoadXml(response);

                // get the job ID
                var ids = doc.GetElementsByTagName("id");
                var id = (ids.Count > 0) ? doc.GetElementsByTagName("id")[0].InnerText : "";

                retval = id;

                doc = null;
            }
            catch (Exception ex) {
                if (ex.Message.Contains("REQUEST_LIMIT_EXCEEDED")) {
                    Utility.Email.SendMail("support@coreinsightgroup.com", "vps@coreinsightgroup.com", "OTP Data Exporter Failed", ex.Message + ": " + ex.StackTrace);
                }
                WriteToLog(ex.Message + ex.StackTrace, Constants.LogSeverityEnum.Error);
            }

            return retval;
        }
        public string CreateQueryBatch(string jobid, string sql) {

            string retval = string.Empty;

            try {

                string createJobUrl = this.BaseURL + "/services/async/29.0/job/" + jobid + "/batch";
                string contentType = "application/xml; charset=UTF-8";

                // add token to header
                List<OTP.Utility.HttpHeader> headers = new List<HttpHeader>();
                HttpHeader header = new HttpHeader();
                header.Name = "X-SFDC-Session";
                header.Value = this.SessionId;
                headers.Add(header);

                // create POST body
                string postData = sql;

                // send POST request
                string response = Http.SendHttpPost(createJobUrl, postData, headers, contentType);
                //WriteToLog(response);

                // process XML response
                var doc = new System.Xml.XmlDocument();
                doc.LoadXml(response);

                // get the request ID
                var ids = doc.GetElementsByTagName("id");
                var id = (ids.Count > 0) ? doc.GetElementsByTagName("id")[0].InnerText : "";

                retval = id;

                doc = null;
            }
            catch (Exception ex) {
                WriteToLog(ex.Message + ex.StackTrace, Constants.LogSeverityEnum.Error);
            }

            return retval;
        }
        public string CheckBatchStatus(string jobid, string batchid) {

            string retval = string.Empty;

            try {

                string createJobUrl = this.BaseURL + "/services/async/29.0/job/" + jobid + "/batch/" + batchid;
                WriteToLog("CheckBatchStatus url: " + createJobUrl);

                // add token to header
                List<OTP.Utility.HttpHeader> headers = new List<HttpHeader>();
                HttpHeader header = new HttpHeader();
                header.Name = "X-SFDC-Session";
                header.Value = this.SessionId;
                headers.Add(header);

                // send POST request
                string response = Http.SendHttpGet(createJobUrl, headers);
                //WriteToLog(response);

                // process XML response
                var doc = new System.Xml.XmlDocument();
                doc.LoadXml(response);

                // get the request ID
                var states = doc.GetElementsByTagName("state");
                var state = (states.Count > 0) ? doc.GetElementsByTagName("state")[0].InnerText : "";

                if (state.Equals("Failed"))
                {
                    WriteToLog("jobid: " + jobid + ", batchid: " + batchid + " failed: " + response);
                }

                retval = state;

                doc = null;
            }
            catch (Exception ex) {
                if (ex.Message.Contains("(400) Bad Request")) {
                    retval = "400Error";
                }
                WriteToLog(ex.Message + ex.StackTrace, Constants.LogSeverityEnum.Error);
            }

            return retval;
        }
        public string GetBatchResultsLink(string jobid, string batchid) {

            string retval = string.Empty;

            try {

                string createJobUrl = this.BaseURL + "/services/async/29.0/job/" + jobid + "/batch/" + batchid + "/result";

                // add token to header
                List<OTP.Utility.HttpHeader> headers = new List<HttpHeader>();
                HttpHeader header = new HttpHeader();
                header.Name = "X-SFDC-Session";
                header.Value = this.SessionId;
                headers.Add(header);

                // send POST request
                string response = Http.SendHttpGet(createJobUrl, headers);

                // process XML response
                var doc = new System.Xml.XmlDocument();
                doc.LoadXml(response);

                // get the request ID
                var results = doc.GetElementsByTagName("result");
                var result = (results.Count > 0) ? doc.GetElementsByTagName("result")[0].InnerText : "";

                retval = result;

                doc = null;
            }
            catch (Exception ex) {
                WriteToLog(ex.Message + ex.StackTrace, Constants.LogSeverityEnum.Error);
            }

            return retval;
        }
        public string GetBatchResults(string jobid, string batchid, string resultid, string type) {

            string retval = string.Empty;

            try {

                string createJobUrl = this.BaseURL + "/services/async/29.0/job/" + jobid + "/batch/" + batchid + "/result/" + resultid;

                // add token to header
                List<OTP.Utility.HttpHeader> headers = new List<HttpHeader>();
                HttpHeader header = new HttpHeader();
                header.Name = "X-SFDC-Session";
                header.Value = this.SessionId;
                headers.Add(header);

                // send POST request
                string response = Http.SendHttpGet(createJobUrl, headers);
                WriteToLog("GetBatchResults SendHttpGet returned...");

                System.Web.Script.Serialization.JavaScriptSerializer serializer = new System.Web.Script.Serialization.JavaScriptSerializer();
                serializer.MaxJsonLength = Int32.MaxValue;
                string json = serializer.Serialize(response);
                WriteToLog("GetBatchResults json Serialized...");

                // process XML response
                var doc = new System.Xml.XmlDocument();
                doc.LoadXml(response);
                WriteToLog("GetBatchResults LoadXml done...");

                // parse entities from xml
                System.Xml.XmlNamespaceManager manager = new System.Xml.XmlNamespaceManager(doc.NameTable);
                manager.AddNamespace("sf", "http://www.force.com/2009/06/asyncapi/dataload");

                System.Xml.XmlNodeList nodes = doc.SelectNodes("//sf:records", manager);
                doc = null;

                WriteToLog("Looping through nodes...");

                foreach (System.Xml.XmlNode node in nodes) {

                    //string jsonText = Newtonsoft.Json.JsonConvert.SerializeObject(node, typeof(Common.Entities.AccountEntity), null);
                    string jsonText = Newtonsoft.Json.JsonConvert.SerializeObject(node);

                    //.SerializeXmlNode(nodes);
                    jsonText = jsonText.Replace("{\"@xsi:nil\":\"true\"}", "\"\"");
                    jsonText = jsonText.Replace("\"@xsi:type\":\"sObject\"", "");
                    jsonText = jsonText.Replace("\"records\":{,", "");
                    int lidx = jsonText.LastIndexOf("}");
                    jsonText = jsonText.Remove(lidx);
                    int idx = jsonText.IndexOf("[");
                    if (idx != -1) {
                        int nidx = jsonText.IndexOf(",", idx + 1);
                        jsonText = jsonText.Remove(idx, ((nidx - idx) + 1));
                        jsonText = jsonText.Replace("]", "");
                    }

                    //WriteToLog("GetBatchResults SerializeObject done...");

                    switch (type) {
                        case "Account":
                            // convert to entity
                            Common.Entities.AccountEntity entity = serializer.Deserialize<Common.Entities.AccountEntity>(jsonText);

                            /*
                            // call out to Google API for Latitude/Longitude
                            try {
                                string address = Utility.Http.UrlEncode(entity.BillingStreet + ", " + entity.BillingCity + ", " + entity.BillingState + " " + entity.BillingPostalCode);
                                string url = String.Format("https://maps.googleapis.com/maps/api/geocode/json?address={0}&sensor=true&key=AIzaSyB7gRBbYPdN6B2YEpHrfsYVDAKaK2EMlUg", address);
                                string geocode = Utility.Http.SendHttpGet(url);
                                Results result = serializer.Deserialize<Results>(geocode);
                                Details details = result.results[0];
                                Geometry geo = details.geometry;
                                entity.Latitude = geo.location.lat;
                                entity.Longitude = geo.location.lng;
                            }
                            catch (Exception exgeo) {
                                //TODO: log error
                            }
                            */

                            // save to database
                            AccountEngine engine = new AccountEngine();
                            DataStructures.BooleanResponse acctbresp = engine.SetAccountEntity(entity);
                            if (!acctbresp.Result) {
                                WriteToLog("SetAccountEntity details failed: " + acctbresp.Error.Message, Constants.LogSeverityEnum.Error);
                            }
                            else
                            {
                                WriteToLog("Save succeeded for Account: " + entity.Id);
                            }
                            entity = null;
                            break;
                        case "Attachment":

                            // convert to entity
                            Common.Entities.AttachmentEntity aentity = serializer.Deserialize<Common.Entities.AttachmentEntity>(jsonText);
                            WriteToLog("GetBatchResults AttachmentEntity generated...");

                            // remove any ID3 tags that may be contained in the mp3 file
                            if (aentity.Body.StartsWith("ID3")) {
                                byte[] a = System.Convert.FromBase64String(aentity.Body);
                                int x = 0;
                                int b = 3;
                                for (int i = 6; i <= 9; i++) {
                                    x += (a[i] << (b * 7));
                                    b--;
                                }
                                byte[] r = new byte[a.Length - x];
                                for (int i = x; i < a.Length; i++) {
                                    r[i - x] = a[i];
                                }
                                aentity.Body = Encoding.Unicode.GetString(r);
                                WriteToLog("GetBatchResults ID3 encoding removed...");
                            }

                            // save to S3
                            aentity.Name = aentity.Name.Replace("/", "_").Replace(" ", "_");
                            UploadToS3(aentity.Body, aentity.Name);
                            WriteToLog("UploadToS3 succeeded for Attachment: " + aentity.Id);

                            AttachmentEngine aengine = new AttachmentEngine();
                            aentity.Body = string.Empty;
                            DataStructures.BooleanResponse attachbresp = aengine.SetAttachmentEntity(aentity);
                            if (!attachbresp.Result) {
                                WriteToLog("SetAttachmentEntity details failed: " + attachbresp.Error.Message, Constants.LogSeverityEnum.Error);
                            }
                            else
                            {
                                WriteToLog("Save succeeded for Attachment: " + aentity.Id);
                            }

                            aentity = null;
                            break;
                        case "Campaign":
                            // convert to entity
                            Common.Entities.CampaignEntity centity = serializer.Deserialize<Common.Entities.CampaignEntity>(jsonText);

                            if (!string.IsNullOrEmpty(centity.Cover_Sheet_Text__c))
                            {
                                StringBuilder sbcoversheet = new StringBuilder();
                                List<string> coversheet = centity.Cover_Sheet_Text__c.Split(new string[] { "\n" }, StringSplitOptions.RemoveEmptyEntries).ToList<string>();
                                if (coversheet != null)
                                {
                                    foreach (string n in coversheet)
                                    {
                                        sbcoversheet.Append("<div>");
                                        sbcoversheet.Append(n);
                                        sbcoversheet.Append("</div>");
                                        sbcoversheet.Append("<br />");
                                    }

                                    centity.Cover_Sheet_Text__c = sbcoversheet.ToString();
                                }
                            }

                            // save to database
                            CampaignEngine cengine = new CampaignEngine();
                            DataStructures.BooleanResponse cbresp = cengine.SetCampaignEntity(centity);
                            if (!cbresp.Result) {
                                WriteToLog("SetCampaignEntity details failed: " + cbresp.Error.Message, Constants.LogSeverityEnum.Error);
                            }
                            else
                            {
                                WriteToLog("Save succeeded for Campaign: " + centity.Id);
                            }
                            centity = null;
                            break;
                        case "Contact":
                            // convert to entity
                            Common.Entities.ContactEntity contactentity = serializer.Deserialize<Common.Entities.ContactEntity>(jsonText);

                            // save to database
                            ContactEngine contactengine = new ContactEngine();
                            DataStructures.BooleanResponse contactbresp = contactengine.SetContactEntity(contactentity);
                            if (!contactbresp.Result) {
                                WriteToLog("SetContactEntity details failed: " + contactbresp.Error.Message, Constants.LogSeverityEnum.Error);
                            }
                            else
                            {
                                WriteToLog("Save succeeded for Contact: " + contactentity.Id);
                            }
                            contactentity = null;
                            break;
                        case "Lead":
                            // convert to entity
                            Common.Entities.LeadEntity leadentity = serializer.Deserialize<Common.Entities.LeadEntity>(jsonText);

                            // save to database
                            LeadEngine leadengine = new LeadEngine();
                            DataStructures.BooleanResponse leadbresp = leadengine.SetLeadEntity(leadentity);
                            if (!leadbresp.Result) {
                                WriteToLog("SetLeadEntity details failed: " + leadbresp.Error.Message, Constants.LogSeverityEnum.Error);
                            }
                            else
                            {
                                WriteToLog("Save succeeded for Lead: " + leadentity.Id);
                            }
                            leadentity = null;
                            break;
                        case "Note":
                            // convert to entity
                            Common.Entities.NoteEntity noteentity = serializer.Deserialize<Common.Entities.NoteEntity>(jsonText);

                            StringBuilder sbnotes = new StringBuilder();
                            List<string> notes = noteentity.Body.Split(new string[] { "\n" }, StringSplitOptions.RemoveEmptyEntries).ToList<string>();
                            if (notes != null)
                            {
                                foreach (string n in notes)
                                {
                                    sbnotes.Append("<div>");
                                    sbnotes.Append(n);
                                    sbnotes.Append("</div>");
                                    sbnotes.Append("<br />");
                                }

                                noteentity.Body = sbnotes.ToString();
                            }

                            // save to database
                            NoteEngine noteengine = new NoteEngine();
                            DataStructures.BooleanResponse notebresp = noteengine.SetNoteEntity(noteentity);
                            if (!notebresp.Result) {
                                WriteToLog("SetNoteEntity details failed: " + notebresp.Error.Message, Constants.LogSeverityEnum.Error);
                            }
                            else
                            {
                                WriteToLog("Save succeeded for Note: " + noteentity.Id);
                            }
                            noteentity = null;
                            break;
                        case "Opportunity":
                            // convert to entity
                            Common.Entities.OpportunityEntity oppentity = serializer.Deserialize<Common.Entities.OpportunityEntity>(jsonText);

                            // save to database
                            OpportunityEngine oppengine = new OpportunityEngine();
                            DataStructures.BooleanResponse oppbresp = oppengine.SetOpportunityEntity(oppentity);
                            if (!oppbresp.Result) {
                                WriteToLog("SetOpportunityEntity details failed: " + oppbresp.Error.Message, Constants.LogSeverityEnum.Error);
                            }
                            else
                            {
                                WriteToLog("Save succeeded for Opportunity: " + oppentity.Id);
                            }
                            oppentity = null;
                            break;
                        case "OpportunityContactRole":
                            // convert to entity
                            Common.Entities.OpportunityContactRoleEntity oppcrentity = serializer.Deserialize<Common.Entities.OpportunityContactRoleEntity>(jsonText);

                            // save to database
                            OpportunityEngine oppcrengine = new OpportunityEngine();
                            DataStructures.BooleanResponse oppcdbresp = oppcrengine.SetOpportunityContactRole(oppcrentity);
                            if (!oppcdbresp.Result)
                            {
                                WriteToLog("SetOpportunityContactRole details failed: " + oppcdbresp.Error.Message, Constants.LogSeverityEnum.Error);
                            }
                            else
                            {
                                WriteToLog("Save succeeded for SetOpportunityContactRole: " + oppcrentity.ContactId + ", " + oppcrentity.OpportunityId);
                            }
                            oppcrentity = null;
                            break;
                        case "Partner":
                            // convert to entity
                            Common.Entities.PartnerEntity pentity = serializer.Deserialize<Common.Entities.PartnerEntity>(jsonText);

                            // save to database
                            PartnerEngine pengine = new PartnerEngine();
                            DataStructures.BooleanResponse pbresp = pengine.SetPartnerEntity(pentity);
                            if (!pbresp.Result) {
                                WriteToLog("SetPartnerEntity details failed: " + pbresp.Error.Message, Constants.LogSeverityEnum.Error);
                            }
                            else
                            {
                                WriteToLog("Save succeeded for Partner: " + pentity.Id);
                            }
                            pentity = null;
                            break;
                        case "Period":
                            // convert to entity
                            Common.Entities.PeriodEntity periodentity = serializer.Deserialize<Common.Entities.PeriodEntity>(jsonText);

                            // save to database
                            PeriodEngine periodengine = new PeriodEngine();
                            DataStructures.BooleanResponse periodbresp = periodengine.SetPeriodEntity(periodentity);
                            if (!periodbresp.Result) {
                                WriteToLog("SetPeriodEntity details failed: " + periodbresp.Error.Message, Constants.LogSeverityEnum.Error);
                            }
                            else
                            {
                                WriteToLog("Save succeeded for Period: " + periodentity.Id);
                            }
                            periodentity = null;
                            break;
                        case "Task":
                            // convert to entity
                            Common.Entities.TaskEntity taskentity = serializer.Deserialize<Common.Entities.TaskEntity>(jsonText);

                            // save to database
                            TaskEngine taskengine = new TaskEngine();
                            DataStructures.BooleanResponse taskbresp = taskengine.SetTaskEntity(taskentity);
                            if (!taskbresp.Result) {
                                WriteToLog("SetTaskEntity details failed: " + taskbresp.Error.Message, Constants.LogSeverityEnum.Error);
                            }
                            else
                            {
                                WriteToLog("Save succeeded for Task: " + taskentity.Id);
                            }
                            taskentity = null;
                            break;
                    }

                    jsonText = null;
                }

                WriteToLog("GetBatchResults completed...");

                retval = response;
            }
            catch (Exception ex) {
                WriteToLog(ex.Message + ex.StackTrace, Constants.LogSeverityEnum.Error);
            }

            return retval;
        }

        private void UploadToS3(string base64String, string fileName)
        {
            try
            {
                string _awsAccessKey = ConfigurationManager.AppSettings["awsAccessKey"].ToString();
                if(string.IsNullOrEmpty(_awsAccessKey)) _awsAccessKey = "AKIAIVQX43LRORBJ4YVQ";

                string _awsSecretKey = ConfigurationManager.AppSettings["awsSecretKey"].ToString();
                if (string.IsNullOrEmpty(_awsSecretKey)) _awsSecretKey = "4+sZBEA1e2d20GnPtoD2mo7L4kcYZr53ozx7n6Fi";

                string _bucketName = ConfigurationManager.AppSettings["bucketName"].ToString();
                if (string.IsNullOrEmpty(_bucketName)) _bucketName = "1tap-otp";

                Amazon.S3.IAmazonS3 client;
                byte[] bytes = Convert.FromBase64String(base64String);

                Amazon.S3.AmazonS3Config S3Config = new Amazon.S3.AmazonS3Config
                {
                    ServiceURL = "https://s3.amazonaws.com"
                };

                using (client = Amazon.AWSClientFactory.CreateAmazonS3Client(_awsAccessKey, _awsSecretKey, S3Config))
                {
                    var request = new Amazon.S3.Model.PutObjectRequest
                    {
                        BucketName = _bucketName,
                        CannedACL = Amazon.S3.S3CannedACL.PublicRead,
                        Key = string.Format("attachments/{0}", fileName)
                    };
                    using (var ms = new System.IO.MemoryStream(bytes))
                    {
                        request.InputStream = ms;
                        client.PutObject(request);
                    }
                    WriteToLog(fileName + " uploaded");
                }
            }
            catch (Exception ex)
            {
                WriteToLog("UploadToS3 ERROR: " + ex.Message);
            }
        }

        public string CloseJob(string jobid) {

            string retval = string.Empty;

            try {

                string createJobUrl = this.BaseURL + "/services/async/29.0/job/" + jobid;
                string contentType = "application/xml; charset=UTF-8";

                // add token to header
                List<OTP.Utility.HttpHeader> headers = new List<HttpHeader>();
                HttpHeader header = new HttpHeader();
                header.Name = "X-SFDC-Session";
                header.Value = this.SessionId;
                headers.Add(header);

                // create POST body
                string postData = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
                postData += "<jobInfo xmlns=\"http://www.force.com/2009/06/asyncapi/dataload\">";
                postData += "<state>Closed</state>";
                postData += "</jobInfo>";

                // send POST request
                string response = Http.SendHttpPost(createJobUrl, postData, headers, contentType);

                retval = response;
            }
            catch (Exception ex) {
                WriteToLog(ex.Message + ex.StackTrace, Constants.LogSeverityEnum.Error);
            }

            return retval;
        }

        public void WriteToLog(string logMessage) {
            WriteToLog(logMessage, Constants.LogSeverityEnum.Verbose);
        }
        public void WriteToLog(string logMessage, string method)
        {
            WriteToLog(logMessage, Constants.LogSeverityEnum.Verbose);
        }
        public void WriteToLog(string logMessage, Common.Constants.LogSeverityEnum level)
        {
            WriteToLog(logMessage, string.Empty, level);
        }
        public void WriteToLog(string logMessage, string method, Common.Constants.LogSeverityEnum level)
        {
            try {
                logger.Error(logMessage);
                //Logger.Write(logMessage);
            }
            catch (Exception ex) {
                Logger.Write(ex.Message);
            }
        }

        public struct Location {
            public string lat;
            public string lng;
        }
        public struct Geometry {
            public Location location;
        }
        public struct Details {
            public Geometry geometry;
        }
        public struct Results {
            public Details[] results;
            public string status;
        }

        #region REST API (single call model)
        /*
        private void ProcessDataREST(object stateInfo) {
            try {
                WriteToLog("Processing OTP Salesforce records...");

                // read settings from config file
                string client_id = ConfigurationManager.AppSettings["client_id"].ToString(); // "3MVG9iTxZANhwHQtd0X2JjFJ8egJMTRGRgYg4zp.v7J31D0oQSLYuvBYXGeN4bALzY2vwOVG8mceUsEESZtep";
                string client_secret = ConfigurationManager.AppSettings["client_secret"].ToString(); //"3867785473303240679";
                string username = ConfigurationManager.AppSettings["username"].ToString(); //"bob.drozdowski%40gmail.com";
                string password = ConfigurationManager.AppSettings["password"].ToString(); //"Ontap123I2eCm8qmFdKeU5X0mBDRD8s2";

                // retrieve token each time to ensure valid token
                string token_url = "https://login.salesforce.com/services/oauth2/token";
                string token_data = string.Format("grant_type=password&client_id={0}&client_secret={1}&username={2}&password={3}", client_id, client_secret, username, password);
                string response = Http.SendHttpPost(token_url, token_data);
                var serializer = new System.Web.Script.Serialization.JavaScriptSerializer();
                Common.Entities.APITokenEntity tokenEntity = serializer.Deserialize<Common.Entities.APITokenEntity>(response);

                // set url values from response
                string instance = tokenEntity.instance_url;
                string token = tokenEntity.access_token;
                string baseURL = instance + "/services/data/v29.0";

                // add token to header
                List<OTP.Utility.HttpHeader> headers = new List<HttpHeader>();
                HttpHeader header = new HttpHeader();
                header.Name = "Authorization";
                header.Value = "Bearer " + token;
                headers.Add(header);

                WriteToLog("Processing Accounts...");
                ProcessAccounts(baseURL, headers);

                WriteToLog("Processing Attachments...");
                ProcessAttachments(baseURL, headers);

                WriteToLog("Processing Campaigns...");
                ProcessCampaigns(baseURL, headers);

                WriteToLog("Processing Contacts...");
                ProcessContacts(baseURL, headers);

                WriteToLog("Processing Leads...");
                ProcessLeads(baseURL, headers);

                WriteToLog("Processing Notes...");
                ProcessNotes(baseURL, headers);

                WriteToLog("Processing Opportunities...");
                ProcessOpportunities(baseURL, headers);

                WriteToLog("Processing Partners...");
                ProcessPartners(baseURL, headers);

                WriteToLog("Processing Periods...");
                ProcessPeriods(baseURL, headers);

                WriteToLog("Processing OTP Salesforce complete...");
            }
            catch (Exception ex) {
                Log log = new Log();
                log.Level = Constants.LogSeverityEnum.Error;
                log.Message = ex.Message;
                log.Trace = ex.StackTrace;
                log.Method = ex.Source;
                WriteToLog("ReadUserStreams failed: " + ex.Message + " " + ex.StackTrace);
                LoggingEngine.Log(log);
            }
        }

        public void ProcessAccounts(string baseURL, List<OTP.Utility.HttpHeader> headers) {

            string url = baseURL + "/query/?q=SELECT+id+from+Account";
            //url = baseURL + "sobjects/Account/";

            string response = Http.SendHttpGet(url, headers);
            var serializer = new System.Web.Script.Serialization.JavaScriptSerializer();
            Common.Entities.APIBaseResponseEntity val = serializer.Deserialize<Common.Entities.APIBaseResponseEntity>(response);

            WriteToLog(val);

            AccountEngine engine = new AccountEngine();

            foreach (Common.Entities.APIBaseEntity apientity in val.records) {

                // retrieve details for Entity
                string detailURL = string.Format("{0}/sobjects/Account/{1}", baseURL, apientity.Id);
                string dresponse = Http.SendHttpGet(detailURL, headers);
                Common.Entities.AccountEntity entity = serializer.Deserialize<Common.Entities.AccountEntity>(dresponse);
                DataStructures.BooleanResponse acctbresp = engine.SetAccountEntity(entity);
                if (!acctbresp.Result) {
                    WriteToLog("SetAccountEntity details failed: " + acctbresp.Error.Message);
                }
                else {
                    WriteToLog("SetAccountEntity details saved successfully!");
                }
            }
        }
        public void ProcessAttachments(string baseURL, List<OTP.Utility.HttpHeader> headers) {

            string url = baseURL + "/query/?q=SELECT+id+from+Attachment";

            string response = Http.SendHttpGet(url, headers);
            var serializer = new System.Web.Script.Serialization.JavaScriptSerializer();
            Common.Entities.APIBaseResponseEntity val = serializer.Deserialize<Common.Entities.APIBaseResponseEntity>(response);

            WriteToLog(val);

            AttachmentEngine engine = new AttachmentEngine();

            foreach (Common.Entities.APIBaseEntity apientity in val.records) {

                // retrieve details for Entity
                string detailURL = string.Format("{0}/sobjects/Attachment/{1}", baseURL, apientity.Id);
                string dresponse = Http.SendHttpGet(detailURL, headers);
                Common.Entities.AttachmentEntity entity = serializer.Deserialize<Common.Entities.AttachmentEntity>(dresponse);
                DataStructures.BooleanResponse acctbresp = engine.SetAttachmentEntity(entity);
                if (!acctbresp.Result) {
                    WriteToLog("SetAttachmentEntity details failed: " + acctbresp.Error.Message);
                }
                else {
                    WriteToLog("SetAttachmentEntity details saved successfully!");
                }
            }
        }
        public void ProcessCampaigns(string baseURL, List<OTP.Utility.HttpHeader> headers) {

            string url = baseURL + "/query/?q=SELECT+id+from+Campaign";

            string response = Http.SendHttpGet(url, headers);
            var serializer = new System.Web.Script.Serialization.JavaScriptSerializer();
            Common.Entities.APIBaseResponseEntity val = serializer.Deserialize<Common.Entities.APIBaseResponseEntity>(response);

            WriteToLog(val);

            CampaignEngine engine = new CampaignEngine();

            foreach (Common.Entities.APIBaseEntity apientity in val.records) {

                // retrieve details for Entity
                string detailURL = string.Format("{0}/sobjects/Campaign/{1}", baseURL, apientity.Id);
                string dresponse = Http.SendHttpGet(detailURL, headers);
                Common.Entities.CampaignEntity entity = serializer.Deserialize<Common.Entities.CampaignEntity>(dresponse);
                DataStructures.BooleanResponse acctbresp = engine.SetCampaignEntity(entity);
                if (!acctbresp.Result) {
                    WriteToLog("SetCampaignEntity details failed: " + acctbresp.Error.Message);
                }
                else {
                    WriteToLog("SetCampaignEntity details saved successfully!");
                }
            }
        }
        public void ProcessContacts(string baseURL, List<OTP.Utility.HttpHeader> headers) {

            string url = baseURL + "/query/?q=SELECT+id+from+Contact"; //+WHERE+SystemModstamp+>+2014-02-22";

            string response = Http.SendHttpGet(url, headers);
            var serializer = new System.Web.Script.Serialization.JavaScriptSerializer();
            Common.Entities.APIBaseResponseEntity val = serializer.Deserialize<Common.Entities.APIBaseResponseEntity>(response);

            WriteToLog(val);

            ContactEngine engine = new ContactEngine();

            foreach (Common.Entities.APIBaseEntity apientity in val.records) {

                // retrieve details for Entity
                string detailURL = string.Format("{0}/sobjects/Contact/{1}", baseURL, apientity.Id);
                string dresponse = Http.SendHttpGet(detailURL, headers);
                Common.Entities.ContactEntity entity = serializer.Deserialize<Common.Entities.ContactEntity>(dresponse);
                DataStructures.BooleanResponse acctbresp = engine.SetContactEntity(entity);
                if (!acctbresp.Result) {
                    WriteToLog("SetContactEntity details failed: " + acctbresp.Error.Message);
                }
                else {
                    WriteToLog("SetContactEntity details saved successfully!");
                }
            }
        }
        public void ProcessLeads(string baseURL, List<OTP.Utility.HttpHeader> headers) {

            string url = baseURL + "/query/?q=SELECT+id+from+Lead";

            string response = Http.SendHttpGet(url, headers);
            var serializer = new System.Web.Script.Serialization.JavaScriptSerializer();
            Common.Entities.APIBaseResponseEntity val = serializer.Deserialize<Common.Entities.APIBaseResponseEntity>(response);

            WriteToLog(val);

            LeadEngine engine = new LeadEngine();

            foreach (Common.Entities.APIBaseEntity apientity in val.records) {

                // retrieve details for Entity
                string detailURL = string.Format("{0}/sobjects/Lead/{1}", baseURL, apientity.Id);
                string dresponse = Http.SendHttpGet(detailURL, headers);
                Common.Entities.LeadEntity entity = serializer.Deserialize<Common.Entities.LeadEntity>(dresponse);
                DataStructures.BooleanResponse acctbresp = engine.SetLeadEntity(entity);
                if (!acctbresp.Result) {
                    WriteToLog("SetLeadEntity details failed: " + acctbresp.Error.Message);
                }
                else {
                    WriteToLog("SetLeadEntity details saved successfully!");
                }
            }
        }
        public void ProcessNotes(string baseURL, List<OTP.Utility.HttpHeader> headers) {

            string url = baseURL + "/query/?q=SELECT+id+from+Note";

            string response = Http.SendHttpGet(url, headers);
            var serializer = new System.Web.Script.Serialization.JavaScriptSerializer();
            Common.Entities.APIBaseResponseEntity val = serializer.Deserialize<Common.Entities.APIBaseResponseEntity>(response);

            WriteToLog(val);

            NoteEngine engine = new NoteEngine();

            foreach (Common.Entities.APIBaseEntity apientity in val.records) {

                // retrieve details for Entity
                string detailURL = string.Format("{0}/sobjects/Note/{1}", baseURL, apientity.Id);
                string dresponse = Http.SendHttpGet(detailURL, headers);
                Common.Entities.NoteEntity entity = serializer.Deserialize<Common.Entities.NoteEntity>(dresponse);
                DataStructures.BooleanResponse acctbresp = engine.SetNoteEntity(entity);
                if (!acctbresp.Result) {
                    WriteToLog("SetNoteEntity details failed: " + acctbresp.Error.Message);
                }
                else {
                    WriteToLog("SetNoteEntity details saved successfully!");
                }
            }
        }
        public void ProcessOpportunities(string baseURL, List<OTP.Utility.HttpHeader> headers) {

            string url = baseURL + "/query/?q=SELECT+id+from+Opportunity";

            string response = Http.SendHttpGet(url, headers);
            var serializer = new System.Web.Script.Serialization.JavaScriptSerializer();
            Common.Entities.APIBaseResponseEntity val = serializer.Deserialize<Common.Entities.APIBaseResponseEntity>(response);

            WriteToLog(val);

            OpportunityEngine engine = new OpportunityEngine();

            foreach (Common.Entities.APIBaseEntity apientity in val.records) {

                // retrieve details for Entity
                string detailURL = string.Format("{0}/sobjects/Opportunity/{1}", baseURL, apientity.Id);
                string dresponse = Http.SendHttpGet(detailURL, headers);
                Common.Entities.OpportunityEntity entity = serializer.Deserialize<Common.Entities.OpportunityEntity>(dresponse);
                DataStructures.BooleanResponse acctbresp = engine.SetOpportunityEntity(entity);
                if (!acctbresp.Result) {
                    WriteToLog("SetOpportunityEntity details failed: " + acctbresp.Error.Message);
                }
                else {
                    WriteToLog("SetOpportunityEntity details saved successfully!");
                }
            }
        }
        public void ProcessPartners(string baseURL, List<OTP.Utility.HttpHeader> headers) {

            string url = baseURL + "/query/?q=SELECT+id+from+Partner";

            string response = Http.SendHttpGet(url, headers);
            var serializer = new System.Web.Script.Serialization.JavaScriptSerializer();
            Common.Entities.APIBaseResponseEntity val = serializer.Deserialize<Common.Entities.APIBaseResponseEntity>(response);

            WriteToLog(val);

            PartnerEngine engine = new PartnerEngine();

            foreach (Common.Entities.APIBaseEntity apientity in val.records) {

                // retrieve details for Entity
                string detailURL = string.Format("{0}/sobjects/Partner/{1}", baseURL, apientity.Id);
                string dresponse = Http.SendHttpGet(detailURL, headers);
                Common.Entities.PartnerEntity entity = serializer.Deserialize<Common.Entities.PartnerEntity>(dresponse);
                DataStructures.BooleanResponse acctbresp = engine.SetPartnerEntity(entity);
                if (!acctbresp.Result) {
                    WriteToLog("SetPartnerEntity details failed: " + acctbresp.Error.Message);
                }
                else {
                    WriteToLog("SetPartnerEntity details saved successfully!");
                }
            }
        }
        public void ProcessPeriods(string baseURL, List<OTP.Utility.HttpHeader> headers) {

            string url = baseURL + "/query/?q=SELECT+id+from+Period";

            string response = Http.SendHttpGet(url, headers);
            var serializer = new System.Web.Script.Serialization.JavaScriptSerializer();
            Common.Entities.APIBaseResponseEntity val = serializer.Deserialize<Common.Entities.APIBaseResponseEntity>(response);

            WriteToLog(val);

            PeriodEngine engine = new PeriodEngine();

            foreach (Common.Entities.APIBaseEntity apientity in val.records) {

                // retrieve details for Entity
                string detailURL = string.Format("{0}/sobjects/Period/{1}", baseURL, apientity.Id);
                string dresponse = Http.SendHttpGet(detailURL, headers);
                Common.Entities.PeriodEntity entity = serializer.Deserialize<Common.Entities.PeriodEntity>(dresponse);
                DataStructures.BooleanResponse acctbresp = engine.SetPeriodEntity(entity);
                if (!acctbresp.Result) {
                    WriteToLog("SetPeriodEntity details failed: " + acctbresp.Error.Message);
                }
                else {
                    WriteToLog("SetPeriodEntity details saved successfully!");
                }
            }
        }
        */
        #endregion
    }
}
