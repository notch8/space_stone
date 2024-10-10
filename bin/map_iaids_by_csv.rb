#!/usr/bin/env ruby

# NOTE: Usage: bin/map_iaids_by_csv.rb path/to/csv

require 'csv'
require 'json'
require 'fileutils'
require 'pathname'

input_csv_path = Pathname.new(ARGV[0])
raise "No file found at #{input_csv_path}" unless input_csv_path.exist?

csv_raw = CSV.read(input_csv_path)
iaid_index = csv_raw[0].index('iaid')
iaids = csv_raw.map { |row| row[iaid_index].split('|') }
iaids.flatten!
iaids.uniq!
iaids -= ['iaid', ' '] # remove column header and blank values
iaids.sort!

puts "Found #{iaids.size} unique IAIDs"

h = {}
iaids.each { |iaid| h[iaid] = {} }

filename, _ext = input_csv_path.basename.to_s.split('.')
json_filename = "#{filename}IAIDs.json"
json_path = Pathname.new("tmp/#{json_filename}")
FileUtils.mkdir_p(json_path.dirname)

File.open(json_path, 'w') do |file|
  file.puts h.to_json
end

puts "Output: #{json_path}"
