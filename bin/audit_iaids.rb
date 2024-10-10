#!/usr/bin/env ruby

# INFO: Usage: bin/audit_iaids.rb path/to/json

# NOTE: To use this script, you will first need to run bin/map_iaids_by_csv.rb

require_relative '../lib/space_stone'
require 'dotenv'
require 'fileutils'
require 'json'
require 'pathname'
require 'ruby-progressbar'

Dotenv.load('.env.production')

def fetch_filenames_from_ia(iaid, data)
  ia_download_service = SpaceStone::IaDownload.new(id: iaid)
  url = "#{ia_download_service.remote_file_link}/" # trailing slash matters

  response = begin
               HTTParty.get(url, headers: { 'Cookie' => ia_download_service.login_cookies })
             rescue Errno::ECONNREFUSED
               data['status'] = "ERROR -- Couldn't connect to IA"
               false
             end
  return [] if response == false || response.body.nil? || response.body.empty?

  page = Nokogiri::HTML(response.body)
  nodeset = page.css('a[href]')
  hrefs = nodeset.map { |element| element['href'] }
  # %2F is a URL-encoded slash ("/")
  hrefs.grep(/download.*\.jp2$/).map { |file_path| file_path.split('%2F').last }
end

def fetch_filenames_from_s3(prefix)
  @bucket
    .objects(prefix:)
    .entries
    .map { |path| path.key.split('/').last }
    .sort
end

def basenames(paths)
  paths.map { |p| p.split('.')[0] }
end

def log_status(data)
  return if data['status'] == "ERROR -- Couldn't connect to IA"

  data['status'] = if data['ia_files'].nil? || data['ia_files'].empty?
                     'ERROR -- No files found in IA'
                   elsif !data['missing_files'].empty? || !data['missing_ocr'].empty? || !data['missing_thumbnails'].empty?
                     'WARN'
                   else
                     'OK'
                   end
end

# ---

json_path = Pathname.new(ARGV[0])
raise "No file found at #{json_path}" unless json_path.exist?

hash = JSON.parse(File.read(json_path))
@bucket = SpaceStone::S3Service.bucket
logger = Logger.new('tmp/audit_debug.log')
puts "\nTrack progress by tailing tmp/audit_debug.log\n\n"
progressbar = ProgressBar.create(total: hash.keys.size, format: '%a %e %P% Processed: %c from %C')

begin
  hash.each do |iaid, data|
    data['last_checked'] = DateTime.now.strftime('%Y-%m-%dT%H:%M:%S')

    if !data.key?('ia_files') || data['ia_files'].empty?
      data['ia_files'] = fetch_filenames_from_ia(iaid, data)
    end

    data['s3_files'] = fetch_filenames_from_s3("#{iaid}/downloads")
    data['s3_ocr'] = fetch_filenames_from_s3("#{iaid}/ocr")
    data['s3_thumbnails'] = fetch_filenames_from_s3("#{iaid}/thumbnails")

    data['missing_files'] = basenames(data['ia_files']) - basenames(data['s3_files'])
    data['missing_ocr'] = basenames(data['ia_files']) - basenames(data['s3_ocr'])
    data['missing_thumbnails'] = basenames(data['ia_files']) - basenames(data['s3_thumbnails'])

    log_status(data)
    logger.info("#{iaid} -- #{data['status']}")
    progressbar.increment
  end
ensure
  puts "\nBacking up existing data to #{json_path}.bak"
  FileUtils.cp(json_path, "#{json_path}.bak")

  File.open(json_path, 'w') do |file|
    # file.puts JSON.pretty_generate(JSON.parse(hash.to_json))
    file.puts hash.to_json
  end

  puts "\nOutput: #{json_path}"
end
