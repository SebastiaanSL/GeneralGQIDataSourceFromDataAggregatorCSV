namespace GQIIntegrationSPI
{
	using System;
	using System.Collections.Generic;
	using System.Data;
	using System.Linq;
	using GeneralGQIDataSourceFromDataAggregatorCSV;
	using Skyline.DataMiner.Analytics.GenericInterface;

	[GQIMetaData(Name = "General DataAggregator CSV Data source (,)")]
	public class GQIDataSourceFromCSVDouble : IGQIOnInit, IGQIDataSource, IGQIInputArguments
	{
		private static readonly GQIStringArgument _argument = new GQIStringArgument("Path to Aggregator Results folder") { IsRequired = true };
		private static readonly GQIStringArgument _aggregatorJobName = new GQIStringArgument("Job name") { IsRequired = true };
		private static readonly GQIStringArgument _maxDaysToSearch = new GQIStringArgument("Max days to search back") { IsRequired = false, DefaultValue = "7" };

		private String _pathValue;
		private String _jobNameValue;
		private int _maxDaysValue;
		private DataTable _table;
		private IGQILogger _logger;

		public OnInitOutputArgs OnInit(OnInitInputArgs args)
		{
			_logger = args.Logger;
			return null;
		}

		public GQIColumn[] GetColumns()
		{
			List<GQIColumn> ret = new List<GQIColumn>();
			foreach (DataColumn col in _table.Columns)
				ret.Add(new GQIStringColumn(col.ColumnName));

			return ret.ToArray();
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

			var path = AggregatorUtility.GetFilePath(_pathValue, _jobNameValue, _maxDaysValue);
			_table = AggregatorUtility.ReadFile(path, _logger);
			return new OnArgumentsProcessedOutputArgs();
		}

		public GQIPage GetNextPage(GetNextPageInputArgs args)
		{
			var rows = new List<GQIRow>();
			foreach (DataRow dataRow in _table.Rows)
			{
				List<GQICell> cells = new List<GQICell>();
				for (int i = 0; i < _table.Columns.Count; i++)
				{
					var cellValue = dataRow[i]?.ToString() ?? string.Empty;
					cells.Add(new GQICell { Value = cellValue });
				}

				GQIRow row = new GQIRow(cells.ToArray());
				rows.Add(row);
			}

			return new GQIPage(rows.ToArray())
			{
				HasNextPage = false,
			};
		}
	}
}