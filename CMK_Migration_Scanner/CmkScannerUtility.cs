// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ------------------------------------------------------------

namespace CmkScanner
{
    using System;
    using System.CommandLine;

    /// <summary>
    /// This enum provides the possible results of the scanner. This scanner is used to
    /// search in a customer account, in order to prevent or fix issues.
    /// </summary>
    public enum ScannerResult
    {
        /// <summary>
        /// If the customer has at least one document with an ID with a length greater than expected.
        /// </summary>
        CmkMigrationIsInvalid,

        /// <summary>
        /// When no documents with ids bigger than 990 are found.
        /// </summary>
        CmkMigrationIsValid,

        /// <summary>
        /// When an unexpected error happens while migrating the account.
        /// </summary>
        UnexpectedErrorFound,
    }

    /// <summary>
    /// This enum provides the API types available for CMK migration.
    /// </summary>
    public enum CosmosDBApiTypes
    {
        SQL,
        MongoDB,
        Gremlin,
        Table
    }

    /// <summary>
    /// Specify how will connect with the specific cosmos sdk client.
    /// </summary>
    public enum CosmosDBAuthType
    {
        AAD,
        AccountKey,
        ConnectionString,
    }

    /// <summary>
    /// Class that holds the credentials to connect to a CosmosDB account.
    /// Could have only one or more of the properties set, depending on the
    /// AuthType they will be used as requested by the user.
    /// </summary>
    public class CosmosDBCredentialForScanner
    {
        public CosmosDBAuthType AuthType { get; set; }
        public string? Hostname { get; set; }
        public string? AccountKey { get; set; }
        public string? ConnectionString { get; set; }
        public string? TenantId { get; set; }
        public string? ClientId { get; set; }
        public string? ClientSecret { get; set; }
    }
    
    /// <summary>
    /// REST requires per documents query the database and container names.
    /// Therefore, this class is used to store the database and container names.
    /// CosmosClient requires as well to get Container instances.
    /// </summary>
    public class DatabaseAndContainer(string database, string container)
    {
        public string Database { get; set; } = database;
        public string Container { get; set; } = container;
    }

    /// <summary>
    /// Utility class with validation methods and helpers for the scanner.
    /// </summary>
    public static class CmkScannerUtility
    {
        // 1) For AAD, the environment variables are:
        //    CMK_HOSTNAME, CMK_API_TYPE, CMK_AUTH_TYPE, CMK_TENANT_ID, CMK_CLIENT_ID, CMK_CLIENT_SECRET
        // 2) For AccountKey, the environment variables are:
        //    CMK_HOSTNAME, CMK_API_TYPE, CMK_AUTH_TYPE, CMK_ACCOUNT_KEY
        // 3) For ConnectionString, the environment variables are:
        //    CMK_CONNECTION_STRING, CMK_API_TYPE, CMK_AUTH_TYPE
        // Always set as well CMK_API_TYPE and CMK_AUTH_TYPE if you are using environment variables.
        // You can set them by using a .env file or by using the command line.
        // Finally, just run: dotnet run Env
        public static readonly string ENV_HOSTNAME = "CMK_HOSTNAME";
        public static readonly string ENV_CONNECTION_STRING = "CMK_CONNECTION_STRING";
        public static readonly string ENV_ACCOUNT_KEY = "CMK_ACCOUNT_KEY";
        public static readonly string ENV_API_TYPE = "CMK_API_TYPE";
        public static readonly string ENV_AUTH_TYPE = "CMK_AUTH_TYPE";
        public static readonly string ENV_TENANT_ID = "CMK_TENANT_ID";
        public static readonly string ENV_CLIENT_ID = "CMK_CLIENT_ID";
        public static readonly string ENV_CLIENT_SECRET = "CMK_CLIENT_SECRET";

        // Message to show when the inputs are invalid or for description in help command.
        public static readonly string HelpError = "\nRun dotnet run -- -h for more information.\n\nTo know more about the specific command, run: dotnet run -- <option> -h.\nExample: dotnet run -- AAD -h\n";
        public static readonly string EnvVariables = "CMK_API_TYPE (required), CMK_AUTH_TYPE (required), CMK_HOSTNAME, CMK_CONNECTION_STRING, CMK_ACCOUNT_KEY, CMK_TENANT_ID, CMK_CLIENT_ID, CMK_CLIENT_SECRET";
        public static readonly string EnvVariablesInstructions = $"\n\nIf using environment variables, set the required attributes (just like the ones from args mentioned in help) with the following options:\n{EnvVariables}";
        public static readonly string TableLimits = "\n\nIf using Table:\nAAD or AccountKeys.";
        public static readonly string MongoLimits = "\n\nIf using MongoDB Account:\nAccountKeys.";
        public static readonly string SqlAndGremlinLimits = "\n\nIf using SQL or Gremlin:\nAAD, ConnectionString or AccountKeys.";
        public static readonly string ScriptDescription = $"This script is used to scan a CosmosDB account to check if the account is ready for CMK migration.\nYou can connect to your Cosmos DB Account with the following authorization options (always use your SQL endpoint):{MongoLimits}{TableLimits}{SqlAndGremlinLimits}\n\nYou can run the script by using arguments or by using environment variables.{EnvVariablesInstructions}";
        public static readonly string InfoLink = "https://learn.microsoft.com/en-us/azure/cosmos-db/how-to-setup-customer-managed-keys-existing-accounts";

        public static RootCommand ManageArgsForCmkScanner(
            Func<CosmosDBAuthType?, CosmosDBApiTypes?, CosmosDBCredentialForScanner?, Task> runScannerFunction)
        {
            // Define the options that the user can write in the command line.
            Option<CosmosDBApiTypes?> apiOption = GetApiTypeOption();
            var connectionStringOption = GetStringOption(
                "--connection-string",
                "The connection string of the Cosmos DB account. Required if using ConnectionString auth type.");
            var hostnameOption = GetStringOption(
                "--host-name",
                "The hostname of the Cosmos DB account. Must be your SQL endpoint, no matter the api type. Required if using AAD or AccountKey auth type.");
            var accountKeyOption = GetStringOption(
                "--account-key",
                "The account key of the Cosmos DB account. Required if using AccountKey auth type.");
            var tenantIdOption = GetStringOption(
                "--tenant-id",
                "The tenant ID of the Cosmos DB account. Required if using AAD auth type.");
            var clientIdOption = GetStringOption(
                "--client-id",
                "The client ID of the Cosmos DB account. Required if using AAD auth type.");
            var clientSecretOption = GetStringOption(
                "--client-secret",
                "The client secret value of the Cosmos DB account. Required if using AAD auth type.");

            // Define the commands, with the expected arguments.
            Command usingEnvVariables = 
                new("Env", "Run Scanner using Environment Variables. You will still have to select one of the earlier authorization methods.") { };
            Command aadCredentials = new("AAD", "Run Scanner using AAD Credentials")
            {
                apiOption,
                hostnameOption,
                tenantIdOption,
                clientIdOption,
                clientSecretOption
            };
            Command connectionStringCredentials = new("ConnectionString", "Run Scanner using Connection String Credentials")
            {
                apiOption,
                connectionStringOption
            };
            Command accountKeyCredentials = new("AccountKey", "Run Scanner using Account Key Credentials")
            {
                apiOption,
                hostnameOption,
                accountKeyOption
            };
            // Define the handlers for the commands. Once the command is selected, the handler will be called.
            CosmosDBCredentialForScanner? credentials;
            usingEnvVariables.SetHandler(async () =>
            {
                credentials = null;
                // Parse the auth type.
                string authTypeString = Environment.GetEnvironmentVariable(ENV_AUTH_TYPE);
                bool isAuthTypeValid = 
                    Enum.TryParse(authTypeString, true, out CosmosDBAuthType authType);
                // Parse the api type.
                string apiTypeString = Environment.GetEnvironmentVariable(ENV_API_TYPE);
                bool isApiTypeValid = 
                    Enum.TryParse(apiTypeString, true, out CosmosDBApiTypes apiType);

                await runScannerFunction(
                    isAuthTypeValid ? authType : null,
                    isApiTypeValid ? apiType : null,
                    credentials);
            });
            aadCredentials.SetHandler(async (apiType, hostname, tenantId, clientId, clientSecret) =>
            {
                credentials = new()
                {
                    Hostname = hostname,
                    TenantId = tenantId,
                    ClientId = clientId,
                    ClientSecret = clientSecret
                };
                await runScannerFunction(CosmosDBAuthType.AAD, apiType, credentials);
            },
            apiOption, hostnameOption, tenantIdOption, clientIdOption, clientSecretOption);
            connectionStringCredentials.SetHandler(async (apiType, connectionString) =>
            {
                credentials = new()
                {
                    ConnectionString = connectionString
                };
                await runScannerFunction(CosmosDBAuthType.ConnectionString, apiType, credentials);
            },
            apiOption, connectionStringOption);
            accountKeyCredentials.SetHandler(async (apiType, hostname, accountKey) =>
            {
                credentials = new()
                {
                    Hostname = hostname,
                    AccountKey = accountKey
                };
                await runScannerFunction(CosmosDBAuthType.AccountKey, apiType, credentials);
            },
            apiOption, hostnameOption, accountKeyOption);
            // Returns the root command with the commands and options.
            return new(ScriptDescription)
            {
                aadCredentials,
                connectionStringCredentials,
                accountKeyCredentials,
                usingEnvVariables
            };
        }

        /// <summary>
        /// Shows a more friendly message to the user.
        /// </summary>
        /// <param name="message">Message to show</param>
        /// <param name="color">Text color</param>
        public static void WriteScannerUpdate(
            string message,
            ConsoleColor color = ConsoleColor.White)
        {
            try
            {
                Console.ForegroundColor = color;
                Console.WriteLine(message);
            }
            finally
            {
                Console.ForegroundColor = ConsoleColor.White;
            }
        }

        /// <summary>
        ///  Validates that the credential object is valid and have the expected info given the
        ///  auth and api type.
        /// </summary>
        /// <param name="credentials">Credentials to authenticate user to the Cosmos Account.
        /// Will be validated in here.</param>
        /// <param name="authType">Auth type to connect to the Cosmos Account. See
        /// CosmosDBAuthType for guidance</param>
        /// <returns></returns>
        public static bool AreInputValuesValid(
            CosmosDBCredentialForScanner? credentials,
            CosmosDBAuthType authType,
            CosmosDBApiTypes apiType)
        {
            // Validate current credentials.
            if (credentials == null)
            {
                return false;
            }
            // Mongo only works with AccountKey.
            if (authType != CosmosDBAuthType.AccountKey && apiType == CosmosDBApiTypes.MongoDB)
            {
                WriteScannerUpdate(
                    "ERROR: MongoDB requires AccountKey as auth type.",
                    ConsoleColor.Red);
                WriteScannerUpdate(HelpError, ConsoleColor.Blue);
                return false;
            }
            // Table doesn't work with ConnectionString.
            if (authType == CosmosDBAuthType.ConnectionString && apiType == CosmosDBApiTypes.Table)
            {
                WriteScannerUpdate(
                    "ERROR: Table API requires AccountKey or AAD as auth type.",
                    ConsoleColor.Red);
                WriteScannerUpdate(HelpError, ConsoleColor.Blue);
                return false;
            }
            // Validate required values per authType.
            bool areValuesValid;
            string errorMessage;
            switch (authType)
            {
                case CosmosDBAuthType.AAD:
                    areValuesValid = !string.IsNullOrEmpty(credentials.TenantId) ||
                        !string.IsNullOrEmpty(credentials.ClientId) ||
                        !string.IsNullOrEmpty(credentials.ClientSecret);
                    errorMessage = "ERROR: Please provide a valid TenantId, ClientId and ClientSecret value";
                    break;

                case CosmosDBAuthType.AccountKey:
                    areValuesValid = !string.IsNullOrEmpty(credentials.Hostname)
                        || !string.IsNullOrEmpty(credentials.AccountKey);
                    errorMessage = "ERROR: Please provide a valid hostname and accountKey";
                    break;

                case CosmosDBAuthType.ConnectionString:
                    areValuesValid = !string.IsNullOrEmpty(credentials.ConnectionString);
                    errorMessage = "ERROR: Please provide a valid connectionString";
                    break;

                default:
                    areValuesValid = false;
                    errorMessage =
                        "ERROR: Invalid Auth type. Please use one of the following: AAD, AccountKey or ConnectionString";
                    break;
            }
            // Provide feedback to the user only for final credential validation.
            if (!areValuesValid)
            {
                WriteScannerUpdate(errorMessage, ConsoleColor.Red);
                WriteScannerUpdate(HelpError, ConsoleColor.Blue);
            }
            return areValuesValid;
        }

        public static Option<CosmosDBApiTypes?> GetApiTypeOption()
        {
            return new Option<CosmosDBApiTypes?>(
            name: "--api-type",
            parseArgument: result =>
            {
                // Check value exists
                if (result.Tokens.Count == 0)
                {
                    return null;
                }
                // Parse the value
                if (!Enum.TryParse(result.Tokens[0].Value, true, out CosmosDBApiTypes apiType))
                {
                    return null;
                }
                return apiType;
            },
            isDefault: true,
            description: "Api Type of your Cosmos DB Account");
        }

        public static Option<string?> GetStringOption(
            string command,
            string description)
        {
            return new Option<string?>(
            name: command,
            parseArgument: result =>
            {
                // Check value exists
                if (result.Tokens.Count == 0)
                {
                    return null;
                }
                // Check value is valid.
                if (string.IsNullOrEmpty(result.Tokens[0].Value))
                {
                    return null;
                }
                return result.Tokens[0].Value;
            },
            isDefault: true,
            description: description);
        }
    }
}