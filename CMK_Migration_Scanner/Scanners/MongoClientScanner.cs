// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ------------------------------------------------------------

namespace CmkScanner
{
    using System.Collections.Concurrent;
    using System.Net;
    using System.Net.Http;
    using System.Text.Json;

    /// <summary>
    /// This class corresponds to the scanner that will be used to check if the
    /// account has IDs with a length greater than 990 for all the accounts
    /// of only MongoDB accounts.
    /// This scanner uses Azure CosmosDB for MongoDB SDK to connect to the account
    /// via Account Keys (connection string) or AAD.
    /// </summary>
    public class MongoClientScanner
    {
        public static readonly string ApiVersion = "2018-09-17";
        public static readonly int MAX_ID_LENGTH = 990;
        public static readonly string Query =
            "SELECT VALUE result FROM (SELECT Count(1) AS count FROM c WHERE LENGTH(c.id) > 990) as result";
        public static readonly string GetDatabasesFromAccountUrl = "{0}dbs";
        public static readonly string GetContainersFromDatabaseUrl = "{0}dbs/{1}/colls";
        public static readonly string QueryDocumentsFromContainerUrl = "{0}dbs/{1}/colls/{2}/docs";

        /// <summary>
        ///  Generates the Authorization header for the REST request. The code is taken from:
        ///  https://github.com/Azure-Samples/cosmos-db-rest-samples/blob/main/Program.cs
        ///  More info: https://learn.microsoft.com/en-us/rest/api/cosmos-db/access-control-on-cosmosdb-resources#constructkeytoken
        /// </summary>
        /// <param name="verb">Type of request: GET, PUT...</param>
        /// <param name="resourceType">Resource types: documents (docs),
        /// databases (db)...</param>
        /// <param name="resourceLink">link that relates resourcetypes with the 
        /// specific resources names</param>
        /// <param name="key">Account Key</param>
        /// <returns>The access token generated to make the request</returns>
        public static string GenerateMasterKeyAuthorizationSignature(
            HttpMethod verb,
            string resourceType,
            string resourceLink,
            string date,
            string key)
        {
            var keyType = "master";
            var tokenVersion = "1.0";
            var payload = $"{verb.ToString().ToLowerInvariant()}\n{resourceType.ToString().ToLowerInvariant()}\n{resourceLink}\n{date.ToLowerInvariant()}\n\n";

            var hmacSha256 = new System.Security.Cryptography.HMACSHA256 { Key = Convert.FromBase64String(key) };
            var hashPayload = hmacSha256.ComputeHash(System.Text.Encoding.UTF8.GetBytes(payload));
            var signature = Convert.ToBase64String(hashPayload);
            var authSet = WebUtility.UrlEncode($"type={keyType}&ver={tokenVersion}&sig={signature}");
            return authSet;
        }

        /// <summary>
        /// Query the documents in a container to check if there are documents with big ids
        /// using REST API. Mongo uses REST API because we required to use the SQL endpoint
        /// to get the document ID length accurately.
        /// </summary>
        /// <param name="credentials">Account credentials</param>
        /// <param name="dbAndContainer">Current database and container to query</param>
        /// <param name="cancellationToken">Cancels operation if big id or other unexpected
        /// error is found. </param>
        /// <returns>The result of the query. If there is at least one document with a bid id,
        /// then it will return true</returns>
        /// <exception cref="Exception"></exception>
        public static async Task<bool> SearchDocumentWithBigIdUsingRestAsync(
            CosmosDBCredentialForScanner credentials,
            DatabaseAndContainer dbAndContainer,
            CancellationToken cancellationToken)
        {
            // First makes sure Cancellation Token hasn't been cancelled.
            cancellationToken.ThrowIfCancellationRequested();

            string url = string.Format(
                QueryDocumentsFromContainerUrl,
                credentials.Hostname,
                dbAndContainer.Database,
                dbAndContainer.Container);

            string resourceLink = $"dbs/{dbAndContainer.Database}/colls/{dbAndContainer.Container}";
            using HttpClient httpClient = new();
            string date = DateTime.UtcNow.ToString("r");
            // Clear default and old headers to avoid conflicts.
            httpClient.DefaultRequestHeaders.Clear();

            httpClient.DefaultRequestHeaders.Add(
                "authorization",
                GenerateMasterKeyAuthorizationSignature(
                    HttpMethod.Post, "docs", resourceLink, date, credentials.AccountKey!));
            httpClient.DefaultRequestHeaders.Add("Accept", "application/json");
            httpClient.DefaultRequestHeaders.Add("x-ms-date", date);
            httpClient.DefaultRequestHeaders.Add("x-ms-version", ApiVersion);
            // Required as it is a query.
            httpClient.DefaultRequestHeaders.Add("x-ms-documentdb-isquery", "True");
            // Required as this is a Mongo DB Account that will use SQL Endpoint.
            httpClient.DefaultRequestHeaders.Add("x-ms-cosmos-apitype", "MongoDB");
            // Required because this is a query.
            httpClient.DefaultRequestHeaders.Add("x-ms-documentdb-query-enablecrosspartition", "True");

            // Creates request body with the query.
            Uri requestUri = new(url);
            string requestBody = @$"
                {{  
                ""query"": ""{Query}""
                }}";
            StringContent requestContent = new(requestBody, System.Text.Encoding.UTF8, "application/query+json");
            //NOTE -> this is important. CosmosDB expects a specific Content-Type with no CharSet on a query request.
#pragma warning disable CS8602
            requestContent.Headers.ContentType.CharSet = "";
#pragma warning restore CS8602

            // Sends POST request with the query as body.
            HttpRequestMessage httpRequest = new()
            { Method = HttpMethod.Post, Content = requestContent, RequestUri = requestUri };
            HttpResponseMessage httpResponse =
                await httpClient.SendAsync(httpRequest, cancellationToken: cancellationToken);
            if (!httpResponse.IsSuccessStatusCode)
            {
                throw new Exception(
                    $"Failed to query documents from container {dbAndContainer.Container} in database {dbAndContainer.Database} with status code {httpResponse.StatusCode} and message: {await httpResponse.Content.ReadAsStringAsync(cancellationToken)}");
            }

            // Extract the Documents property from json in the response and save it.
            // Those are the documents that match the query.
            string responseContent = await httpResponse.Content.ReadAsStringAsync(cancellationToken);
            JsonElement responseObject = JsonSerializer.Deserialize<JsonElement>(responseContent);

            // If there are documents with big ids, we will have results.
            int count = responseObject.GetProperty("Documents")[0].GetProperty("count").GetInt32();
            return count > 0;
        }

        /// <summary>
        /// Gets all containers by fetching all databases and then all containers using REST.
        /// </summary>
        /// <param name="httpClient">HTTP Client to make the GET calls</param>
        /// <param name="credentials">User crdentials to connect to their Cosmos DB account</param>
        /// <returns>List of containers and databases names.</returns>
        /// <exception cref="Exception"></exception>
        public static async Task<List<DatabaseAndContainer>> GetAllContainersWithRestAsync(
            HttpClient httpClient,
            CosmosDBCredentialForScanner credentials)
        {
            // Setup async options. ConcurrentBag is thread safe.
            ConcurrentBag<DatabaseAndContainer> containers = [];
            CancellationTokenSource cancellationTokenSource = new();

            // Flag used as integer to be able to use Interlocked.Exchange between threads.
            int unexpectedError = 0;

            // Get all databases from the account.
            List<string> databases = GetAllDatabasesWithRest(
                httpClient,
                credentials,
                cancellationTokenSource.Token);

            // As we were able to get the databases, we can assume the credentials are valid.
            CmkScannerUtility.WriteScannerUpdate("Credentials validated.");

            // Check cancellation token before starting tasks.
            cancellationTokenSource.Token.ThrowIfCancellationRequested();

            //Using Tasks as these are not heavy async operations, only get containers.
            List<Task> tasksToGetContainersPerDb = [];
            foreach (string db in databases)
            {
                async Task task()
                {
                    try
                    {
                        List<string> containersFromDb = await GetContainersFromDatabaseWithRestAsync(
                            httpClient,
                            db,
                            credentials,
                            cancellationTokenSource.Token);

                        foreach (string container in containersFromDb)
                        {
                            // Save the database name, and all the containers names from it.
                            containers.Add(new DatabaseAndContainer(db, container));
                        }
                    }
                    catch (Exception e)
                    {
                        CmkScannerUtility.WriteScannerUpdate(
                            $"ERROR: Unexpected error when fetching containers from database {db}. Exception: {e}",
                            ConsoleColor.Red);

                        // Set flag to 1 to let other tasks know an error happened.
                        Interlocked.Exchange(ref unexpectedError, 1);
                        cancellationTokenSource.Cancel();
                    }

                }

                // Add to task list to wait for all tasks to finish later on.
                tasksToGetContainersPerDb.Add(task());
            }

            // Wait for all tasks to finish.
            await Task.WhenAll(tasksToGetContainersPerDb);

            // If an unexpected error happened, throw exception. Else, return containers.
            return unexpectedError == 1
                ? throw new Exception(
                    "Unexpected error when fetching containers from account.")
                : containers.ToList();
        }

        /// <summary>
        /// Gets all databases from the account using REST API.
        /// </summary>
        /// <param name="httpClient">HTTP Client to make the GET calls</param>
        /// <param name="credentials">User crdentials to connect to their Cosmos DB account</param>
        /// <param name="cancellationToken">Cancellation token in case an error happen</param>
        /// <returns>List of databases names of the account</returns>
        public static List<string> GetAllDatabasesWithRest(
            HttpClient httpClient,
            CosmosDBCredentialForScanner credentials,
            CancellationToken cancellationToken)
        {
            string url = string.Format(
                GetDatabasesFromAccountUrl,
                credentials.Hostname);
            string date = DateTime.UtcNow.ToString("r");

            // Clear default and old headers to avoid conflicts.
            httpClient.DefaultRequestHeaders.Clear();
            httpClient.DefaultRequestHeaders.Add(
                "authorization",
                GenerateMasterKeyAuthorizationSignature(
                    HttpMethod.Get, "dbs", string.Empty, date, credentials.AccountKey!));
            httpClient.DefaultRequestHeaders.Add("x-ms-date", date);
            httpClient.DefaultRequestHeaders.Add("x-ms-version", ApiVersion);
            httpClient.DefaultRequestHeaders.Add("x-ms-cosmos-apitype", "MongoDB");

            string response =
                httpClient.GetStringAsync(new Uri(url), cancellationToken: cancellationToken).Result;

            JsonElement responseObject = JsonSerializer.Deserialize<JsonElement>(response);
            List<string> databases = new();
            foreach (JsonElement db in responseObject.GetProperty("Databases").EnumerateArray())
            {
                string? dbName = db.GetProperty("id").GetString();
                if (!string.IsNullOrEmpty(dbName))
                {
                    databases.Add(item: dbName);
                }
            }
            return databases;
        }

        /// <summary>
        /// Gets all containers from a specific database using REST API.
        /// </summary>
        /// <param name="httpClient">HTTP Client to make the GET calls</param>
        /// <param name="database">Database name to get the containers from</param>
        /// <param name="credentials">User crdentials to connect to their Cosmos DB account</param>
        /// <param name="cancellationToken">Cancellation token in case an error happen</param>
        /// <returns>List of containers names from the specific database.</returns>
        public static async Task<List<string>> GetContainersFromDatabaseWithRestAsync(
            HttpClient httpClient,
            string database,
            CosmosDBCredentialForScanner credentials,
            CancellationToken cancellationToken)
        {
            // Check cancellation token before starting the request.
            cancellationToken.ThrowIfCancellationRequested();

            string url = string.Format(
                GetContainersFromDatabaseUrl,
                credentials.Hostname,
                database);
            string resourceLink = $"dbs/{database}";
            string date = DateTime.UtcNow.ToString("r");

            // Clear default and old headers to avoid conflicts.
            httpClient.DefaultRequestHeaders.Clear();
            httpClient.DefaultRequestHeaders.Add(
                "authorization",
                GenerateMasterKeyAuthorizationSignature(
                    HttpMethod.Get, "colls", resourceLink, date, credentials.AccountKey!));
            httpClient.DefaultRequestHeaders.Add("x-ms-date", date);
            httpClient.DefaultRequestHeaders.Add("x-ms-version", ApiVersion);

            // Sends POST request with the query as body.
            HttpRequestMessage httpRequest = new() { Method = HttpMethod.Get, RequestUri = new Uri(url) };
            HttpResponseMessage response =
                await httpClient.SendAsync(httpRequest, cancellationToken: cancellationToken);
            string responseContent = await response.Content.ReadAsStringAsync(cancellationToken);
            JsonElement responseObject = JsonSerializer.Deserialize<JsonElement>(responseContent);
            List<string> containers = new();
            foreach (JsonElement container in responseObject.GetProperty("DocumentCollections").EnumerateArray())
            {
                string? containerName = container.GetProperty("id").GetString();
                if (!string.IsNullOrEmpty(containerName))
                {
                    containers.Add(item: containerName);
                }
            }
            return containers;
        }

        /// <summary>
        /// By using REST and the SQL endpoint from the Mongo Account, this method will scan
        /// all the documents in the database account to check if there are any documents with
        /// at least one bid id.
        /// </summary>
        /// <param name="credentials">User crdentials to connect to their Cosmos DB account</param>
        /// <returns>Scanner Result that determines if the account can be migrated to CMK</returns>
        public static async Task<ScannerResult> ScanWithMongoWithRestAsync(
            CosmosDBCredentialForScanner credentials)
        {
            CancellationTokenSource migrationFailedTokenSource = new();
            HttpClient httpClient = new();

            // Get all containers and databases from the account.
            List<DatabaseAndContainer> containers;
            try
            {
                containers = await GetAllContainersWithRestAsync(httpClient, credentials);
                CmkScannerUtility.WriteScannerUpdate($"Found {containers.Count} container(s) in the account.");
            }
            catch (Exception e)
            {
                CmkScannerUtility.WriteScannerUpdate(
                    $"ERROR: Unexpected error when fetching containers from account. Exception: {e}",
                    ConsoleColor.Red);
                return ScannerResult.UnexpectedErrorFound;
            }

            // Atomic flags used as integers to be able to use Interlocked.Exchange between threads.
            int unexpectedError = 0;
            int bigIdFound = 0;

            List<Task> tasks = containers.Select(dbAndContainer => Task.Run(async () =>
            {
                try
                {
                    // First check if the token was cancelled. If so, throw an exception to stop the task.
                    if (migrationFailedTokenSource.IsCancellationRequested)
                    {
                        throw new TaskCanceledException();
                    }

                    // Will look for documents with Big Ids. If found one, workflow ends with invalid. 
                    bool wereDocumentsWithBigIdsFound = await SearchDocumentWithBigIdUsingRestAsync(
                        credentials,
                        dbAndContainer,
                        migrationFailedTokenSource.Token);

                    if (wereDocumentsWithBigIdsFound)
                    {
                        // If a big id is found, set the atomic flag to 1 which means true.
                        Interlocked.Exchange(ref bigIdFound, 1);

                        // Cancel all other tasks, no need to keep searching.
                        migrationFailedTokenSource.Cancel();
                    }
                }
                catch (Exception e)
                {
                    // Ignore if the error is a cancellation exception. Another thread already set the flag.
                    if (e is not TaskCanceledException && e is not OperationCanceledException)
                    {
                        // If an unexpected error is found, set this flag to 1 which means true.
                        Interlocked.Exchange(ref unexpectedError, 1);
                        CmkScannerUtility.WriteScannerUpdate(
                            $"ERROR: Unexpected error during CMK Migration Scan. Exception: {e}",
                            ConsoleColor.Red);

                        // Cancel all other tasks no matter the type of error.
                        migrationFailedTokenSource.Cancel();
                    }
                }
            }, migrationFailedTokenSource.Token)).ToList();

            // Wait for all tasks to finish.
            CmkScannerUtility.WriteScannerUpdate("Scanning...");
            await Task.WhenAll(tasks);

            return unexpectedError == 1
                ? ScannerResult.UnexpectedErrorFound
                : bigIdFound == 1
                    ? ScannerResult.CmkMigrationIsInvalid
                    : ScannerResult.CmkMigrationIsValid;
        }

    }
}