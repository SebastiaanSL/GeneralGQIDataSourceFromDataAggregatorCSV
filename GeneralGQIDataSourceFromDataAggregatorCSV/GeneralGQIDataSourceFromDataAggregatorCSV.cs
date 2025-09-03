using Skyline.DataMiner.Analytics.GenericInterface;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;

namespace GQIIntegrationSPI
{
    [GQIMetaData(Name = "General DataAggregator CSV Data source (,)")]
    public class GQIDataSourceFromCSVDouble : IGQIDataSource, IGQIOnInit, IGQIOnDestroy, IGQIInputArguments
    {
        private GQIStringArgument _argument = new GQIStringArgument("Path to Aggregator Results folder") { IsRequired = true };
        private GQIStringArgument _aggregatorJobName = new GQIStringArgument("Job name") { IsRequired = true };
        private GQIStringArgument _maxDaysToSearch = new GQIStringArgument("Max days to search back") { IsRequired = false, DefaultValue = "7" };
        private String _pathValue;
        private String _jobNameValue;
        private int _maxDaysValue;
        private CSVFile _File;

        /// <inheritdoc />
        public GQIColumn[] GetColumns()
        {
            List<GQIColumn> ret = new List<GQIColumn>();
            foreach (String col in _File.Columns)
                ret.Add(new GQIStringColumn(col));

            return ret.ToArray();
        }

        /// <inheritdoc />
        public GQIPage GetNextPage(GetNextPageInputArgs args)
        {
            var rows = new List<GQIRow>();
            foreach (String[] dataRow in _File.Data)
            {
                List<GQICell> cells = new List<GQICell>();
                foreach (String cellValue in dataRow.Take(_File.Columns.Length).ToArray())
                {
                    cells.Add(new GQICell { Value = cellValue });
                }

                while (cells.Count < _File.Columns.Length)
                {
                    cells.Add(new GQICell { Value = "###auto appended###" });
                }

                GQIRow row = new GQIRow(cells.ToArray());
                rows.Add(row);
            }

            return new GQIPage(rows.ToArray())
            {
                HasNextPage = false
            };
        }

        /// <inheritdoc />
        public OnInitOutputArgs OnInit(OnInitInputArgs args)
        {
            return new OnInitOutputArgs();
        }

        public GQIArgument[] GetInputArguments()
        {
            return new GQIArgument[] { _argument, _aggregatorJobName, _maxDaysToSearch };
        }

        public OnArgumentsProcessedOutputArgs OnArgumentsProcessed(OnArgumentsProcessedInputArgs args)
        {
            _pathValue = args.GetArgumentValue(_argument);
            _jobNameValue = args.GetArgumentValue(_aggregatorJobName);

            // Parse the max days value with error handling
            var maxDaysString = args.GetArgumentValue(_maxDaysToSearch);
            if (!int.TryParse(maxDaysString, out _maxDaysValue) || _maxDaysValue <= 0)
            {
                _maxDaysValue = 7; // Default fallback
            }

            _File = new CSVFile(_pathValue, _jobNameValue, _maxDaysValue);
            return new OnArgumentsProcessedOutputArgs();
        }
        /// <inheritdoc />
        public OnDestroyOutputArgs OnDestroy(OnDestroyInputArgs args)
        {
            return new OnDestroyOutputArgs();
        }
    }

    public class CSVFile
    {
        public CSVFile(String path, string fileContainsValue, int maxDaysToSearch = 7)
        {
            path = GetFilePath(path, fileContainsValue, maxDaysToSearch);
            ReadFile(path);
        }

        public string[] Columns { get; private set; }
        public List<string[]> Data { get; private set; }

        private void ReadFile(String filePath)
        {
            if (File.Exists(filePath))
            {
                Data = new List<string[]>();
                // Read all lines from the CSV file
                string[] lines = File.ReadAllLines(filePath);

                // Iterate through each line
                foreach (string line in lines)
                {
                    // Split the line into fields using comma as delimiter
                    string[] fields = WebUtility.HtmlDecode(line.TrimEnd('\t')).Split(',');
                    for (int i = 0; i < fields.Length; i++)
                        fields[i] = fields[i].Trim('"');

                    if (Columns == null)
                        Columns = fields;
                    else
                        Data.Add(fields);
                }
            }
            else
            {
                Columns = new String[] { "Errors" };
                Data = new List<string[]>();
                Data.Add(new String[] { $"File does not exist: {filePath}" });
            }
        }

        private string GetFilePath(string path, string fileContainsValue, int maxDaysToSearch)
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
    }
}