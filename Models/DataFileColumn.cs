using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text.RegularExpressions;

namespace DataFile.Models
{
    public class DataFileColumn
    {
        public string Alias { get; set; }
        public int End { get; set; }
        private object _exampleValue;

        public object ExampleValue
        {
            get
            {
                return _exampleValue;
            }
            set { _exampleValue = ConvertValue(value); }
        }

        public int Start { get; set; }
        public bool Required;
        public string Pattern { get; set; }
        public int? MinLength { get; set; }
        public int? MaxLength { get; set; }

        private Type _dataType;
        public Type DataType
        {
            get { return _dataType; }
            set
            {
                _dataType = value;
                _exampleValue = ConvertValue(_exampleValue);
            }
        }

        public DataFileColumn()
        {

        }

        public DataFileColumn(string name): this()
        {
            Name = name;
        }

        public DataFileColumn(string name, int length)
            : this(name)
        {
            Length = length;
        }

        public int Length { get; set; } = -1;

        public bool LengthSpecified => Length > 0;

        public string Name { get; set; }
        public StringComparison AllowedValuesComparison { get; set; }
        public IEnumerable<string> AllowedValues { get; set; }

        public object ConvertValue(object value)
        {
            if (DataType == null | value == null) return value;
            if (value.GetType() == DataType)
            {
                return value;
            }
            var rtn = Activator.CreateInstance(DataType);
            if (value == rtn)
            {
                return rtn;
            }
            if (Convert.IsDBNull(value)) return rtn;
            
            var converter = TypeDescriptor.GetConverter(DataType);
            if (!converter.IsValid(value))
            {
                return value.ToString();
            }
            return converter.ConvertFrom(value);
        }

        public DataFileValueValidity ValidateValue(object value)
        {
            var validity = new DataFileValueValidity();
            if (DataType != null)
            {
                var convertedValue = ConvertValue(value);
                if (convertedValue != null && convertedValue.GetType() != DataType)
                {
                    validity.Error.DataType = true;
                }
            }
            
            var textValue = value?.ToString() ?? string.Empty;
            if (Required && textValue.Trim().Length == 0)
            {
                validity.Error.Required = true;
            }
            if (MinLength.HasValue && MinLength.Value > 0)
            {
                if (textValue.Length < MinLength.Value)
                {
                    validity.Error.MinLength = true;
                }
            }
            if (MaxLength.HasValue && MaxLength.Value > 0)
            {
                if (textValue.Length > MaxLength.Value)
                {
                    validity.Error.MaxLength = true;
                }
            }
            if (!string.IsNullOrWhiteSpace(textValue))
            {
                if (AllowedValues != null)
                {
                    if (!AllowedValues.Any(allowedValue => allowedValue.Equals(textValue, AllowedValuesComparison)))
                    {
                        validity.Error.AllowedValues = true;
                    }
                }
                if (!string.IsNullOrWhiteSpace(Pattern))
                {
                    var regex = new Regex(Pattern);
                    if (!regex.IsMatch(textValue))
                    {
                        validity.Error.Pattern = true;
                    }
                }

            }
            return validity;
        }
    }
}