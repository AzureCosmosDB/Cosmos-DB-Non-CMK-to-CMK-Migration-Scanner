// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ------------------------------------------------------------

namespace CmkScanner
{
    using System;
    using System.CommandLine;

    /// <summary>
    /// This class starts the process of validating if an account can be
    /// migrated to CMK. Will show a message to the user with the result.
    /// This class scans all the documents of the account provided to check
    /// if there is at least one document with an ID lenght greater than 990.
    /// More info: https://learn.microsoft.com/en-us/azure/cosmos-db/how-to-setup-customer-managed-keys-existing-accounts
    /// </summary>
    public class Program
    {
        /// <summary>
        /// Main method that runs the scanner. The expected inputs are:
        /// 1) If using AAD (which uses Azure CLI auth) you can:
        ///     a) Set the environment variables: CMK_AUTH_TYPE, CMK_HOSTNAME, CMK_API_TYPE and CMK_AUTH_TYPE.
        ///     b) Run the script with the args required by: dotnet run -- AAD -h
        /// 2) If using AccountKey, you can:
        ///     a) Set the environment variables: CMK_AUTH_TYPE, CMK_HOSTNAME, CMK_ACCOUNT_KEY and CMK_API_TYPE.
        ///     b) Run the script with the args required by: dotnet run -- AccountKey -h
        /// 3) If using ConnectionString, you can:
        ///     a) Set the environment variables: CMK_AUTH_TYPE, CMK_CONNECTION_STRING and CMK_API_TYPE.
        ///     b) Run the script with the args required by: dotnet run -- ConnectionString -h
        ///  If using environment variables, you must run the script with: dotnet run Env
        /// Arguments:
        /// 1) authTypeString: The type of the auth to use. Can be AAD or AccountKey. Required.
        /// 2) apiTypeString: The type of the cosmos account. See CosmosDBApiTypes enum for guidance.
        /// 3) hostname: Hostname of the Cosmos DB Account. Always use the SQL endpoint. Required.
        /// 4) connectionString: Connection string of the Cosmos DB Account. Must be valid.
        /// 5) accountKey: Account key of the Cosmos DB Account. Must be valid.
        /// 6) tenantId: The tenant ID of the cosmos account and only required if authType is AAD.
        /// 7) clientId: The client ID of the cosmos account and only required if authType is AAD.
        /// 8) clientSecret: The client secret value and only required if authType is AAD.
        /// </summary>
        static int Main(string[] args)
        {
            RootCommand rootCommand = CmkScannerUtility.ManageArgsForCmkScanner(RunScanner);
            return rootCommand.InvokeAsync(args).Result;
        }

        public static async Task RunScanner(
            CosmosDBAuthType? authType,
            CosmosDBApiTypes? apiType,
            CosmosDBCredentialForScanner? credentials,
            bool useComputedProperties)
        {
            // Check if the user has provided valid inputs.
            if (authType == null)
            {
                CmkScannerUtility.WriteScannerUpdate("ERROR: Invalid auth type.", ConsoleColor.Red);
                CmkScannerUtility.WriteScannerUpdate(CmkScannerUtility.HelpError, ConsoleColor.Blue);
                return;
            }
            if (apiType == null)
            {
                CmkScannerUtility.WriteScannerUpdate("ERROR: Invalid API type.", ConsoleColor.Red);
                CmkScannerUtility.WriteScannerUpdate(CmkScannerUtility.HelpError, ConsoleColor.Blue);
                return;
            }
            // Check credentials.
            if (credentials == null)
            {
                // Only if there are no args data, we will check if the user has set the env variables.
                credentials = new()
                {
                    Hostname = Environment.GetEnvironmentVariable(CmkScannerUtility.ENV_HOSTNAME),
                    ConnectionString = Environment.GetEnvironmentVariable(CmkScannerUtility.ENV_CONNECTION_STRING),
                    AccountKey = Environment.GetEnvironmentVariable(CmkScannerUtility.ENV_ACCOUNT_KEY),
                    TenantId = Environment.GetEnvironmentVariable(CmkScannerUtility.ENV_TENANT_ID),
                    ClientId = Environment.GetEnvironmentVariable(CmkScannerUtility.ENV_CLIENT_ID),
                    ClientSecret = Environment.GetEnvironmentVariable(CmkScannerUtility.ENV_CLIENT_SECRET)
                };
                CmkScannerUtility.WriteScannerUpdate("Using environment variables data...", ConsoleColor.Green);
            }
            else
            {
                CmkScannerUtility.WriteScannerUpdate("Using arguments data...", ConsoleColor.Green);
            }
            // Check if the user has set the computed properties.
            if (useComputedProperties)
            {
                CmkScannerUtility.WriteScannerUpdate("Using computed properties...", ConsoleColor.Green);
            }
            // Check if the user has set the data correctly.
            if (!CmkScannerUtility.AreInputValuesValid(
                credentials,
                (CosmosDBAuthType)authType,
                (CosmosDBApiTypes)apiType))
            {
                return;
            }
            // Update user with valid inputs.
            CmkScannerUtility.WriteScannerUpdate(
                "Input data is valid. Starting to setup the scanner...",
                ConsoleColor.Green);
            try
            {
                // Start the scanner.
                ScannerResult? scannerResult = null;
                switch (apiType)
                {
                    case CosmosDBApiTypes.SQL:
                    case CosmosDBApiTypes.Gremlin:
                    case CosmosDBApiTypes.Table:
                        scannerResult = await CosmosClientScanner.ScanWithCosmosClientAsync(
                            credentials,
                            (CosmosDBAuthType)authType,
                            useComputedProperties);
                        break;
                    case CosmosDBApiTypes.MongoDB:
                        scannerResult = await MongoClientScanner.ScanWithMongoWithRestAsync(credentials);
                        break;
                    default:
                        CmkScannerUtility.WriteScannerUpdate(
                            "ERROR: Invalid API type. Please use one of the following: SQL, MongoDB, Gremlin, Table",
                            ConsoleColor.Red);
                        CmkScannerUtility.WriteScannerUpdate(CmkScannerUtility.HelpError);
                        break;
                }

                // Analyze the result and show a message to the user.
                switch (scannerResult)
                {
                    case ScannerResult.CmkMigrationIsInvalid:
                        CmkScannerUtility.WriteScannerUpdate(
                            $"The account cannot be migrated to CMK because a big id was found. More info: {CmkScannerUtility.InfoLink}.",
                            ConsoleColor.Red);
                        break;
                    
                    case ScannerResult.CmkMigrationIsValid:
                        CmkScannerUtility.WriteScannerUpdate(
                            "The account can be migrated to CMK.",
                            ConsoleColor.Green);
                        break;

                    case ScannerResult.UnexpectedErrorFound:
                        CmkScannerUtility.WriteScannerUpdate(
                            $"An unexpected error happened while scanning the account. Please try again later. More info: {CmkScannerUtility.InfoLink}.",
                            ConsoleColor.Red);
                        break;
                }
            }
            catch (Exception e)
            {
                CmkScannerUtility.WriteScannerUpdate(
                    $"ERROR: Error in general steps before scanning: {e}",
                    ConsoleColor.Red);
            }
            finally {
                // Avoids unnecessary bug colors in the console.
                Console.ForegroundColor = ConsoleColor.White;
            }
        }
    }
}