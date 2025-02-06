#!/usr/bin/env ruby

# INFO: Usage: bin/prep_csvs.rb path/to/base_csv path/to/audit_json

require 'csv'
require 'json'
require 'pathname'

input_csv_path = Pathname.new(ARGV[0])
raise "No file found at #{input_csv_path}" unless input_csv_path.exist?

input_json_path = Pathname.new(ARGV[1])
raise "No file found at #{input_json_path}" unless input_json_path.exist?

h = JSON.parse(File.read(input_json_path))
ready_to_import = h.select { |_iaid, data| data['status'] == 'OK' }
ready_iaids = ready_to_import.keys

csv_data = CSV.read(input_csv_path, headers: true)
# Grab rows where all of the IAIDs have all of their files in S3 OR where IAID is empty
ready_rows = csv_data.select do |row|
  row_iaids = row['iaid'].split('|').compact.map(&:strip).reject(&:empty?)
  row_iaids.all? do |iaid|
    ready_iaids.include?(iaid)
  end
end

total_rows = ready_rows.size
rows_per_file = 1_000
num_of_files = (total_rows.to_f / rows_per_file).ceil

num_of_files.times do |i|
  start_index = i * rows_per_file
  end_index = [start_index + rows_per_file - 1, total_rows - 1].min

  subset = ready_rows[start_index..end_index]

  new_csv_path = "tmp/#{input_csv_path.basename.sub(/.csv$/, '')}_#{Date.today.strftime('%Y-%m-%d')}_#{i + 1}.csv"

  # Write the subset to a new CSV file, including the headers
  CSV.open(new_csv_path, 'w') do |csv|
    csv << csv_data.headers
    subset.each do |row|
      csv << row
    end
  end

  puts "Created #{new_csv_path} with rows #{start_index + 1} to #{end_index + 1}"
end
