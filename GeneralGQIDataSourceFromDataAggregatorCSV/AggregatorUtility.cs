namespace GeneralGQIDataSourceFromDataAggregatorCSV
{
	using System;
	using System.Data;
	using System.Globalization;
	using System.IO;
	using System.Linq;
	using CsvHelper;
	using CsvHelper.Configuration;
	using Skyline.DataMiner.Analytics.GenericInterface;

	internal static class AggregatorUtility
	{
		internal static string GetFilePath(string path, string fileContainsValue, int maxDaysToSearch)
		{
			var currentDate = DateTime.Now;

			for (int daysBack = 0; daysBack < maxDaysToSearch; daysBack++)
			{
				var searchDate = currentDate.AddDays(-daysBack);
				var fullPath = $"{path}\\{searchDate.Year}\\{searchDate.Month:D2}\\{searchDate.Day:D2}\\";

				// Check if the directory exists before trying to access it
				if (!Directory.Exists(fullPath))
				{
					continue; // Try the next day
				}

				var directory = new DirectoryInfo(fullPath);
				var matchingFiles = directory.GetFiles()
					.Where(x => x.Name.Contains(fileContainsValue))
					.OrderByDescending(f => f.LastWriteTime);

				// If we found matching files, return the most recent one
				if (matchingFiles.Any())
				{
					return matchingFiles.First().FullName;
				}
			}

			// If no file was found after searching multiple days, return a fallback path
			// This will be handled by the ReadFile method which checks for file existence
			return $"{path}\\FileNotFound_{fileContainsValue}_{DateTime.Now:yyyyMMdd}.csv";
		}

		internal static DataTable ReadFile(String filePath, IGQILogger logger)
		{
			if (!File.Exists(filePath))
				throw new GenIfException($"File not found: '{filePath}'.");

			var config = new CsvConfiguration(CultureInfo.InvariantCulture)
			{
				Delimiter = ",",
				HasHeaderRecord = true,
				LineBreakInQuotedFieldIsBadData = false,
				BadDataFound = null, // Ignore bad data lines
				MissingFieldFound = null, // Ignore missing fields
			};

			logger.Information($"Reading CSV file: {filePath}");
			using (var reader = new StreamReader(filePath))
			using (var csv = new CsvReader(reader, config))
			{
				using (var dr = new CsvDataReader(csv))
				{
					var dataTable = new DataTable();
					dataTable.Load(dr);
					return dataTable;
				}
			}
		}
	}
}
