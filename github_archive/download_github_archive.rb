require 'open-uri'
open('image.png', 'wb') do |file|
  file << open('http://data.githubarchive.org/2012-04-11-0.json.gz').read
end