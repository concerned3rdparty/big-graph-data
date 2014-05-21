require File.expand_path(File.dirname(__FILE__) + '/config')

(bname, bpath, filename) = ARGV
unless bname && bpath && filename
	puts "Usage: ruby s3uploadefile.rb <bname> <bpath> <filename>"
	exit 1
end

s3 = AWS::S3.new

begin
	b = s3.buckets[bname]
	basename = File.basename(filename)
	o = b.objects[bpath + basename]
	o.write(:file => filename)
rescue Exception => e
	puts "An error occured with the following message:"
	puts "-----------------------------------------------------"
	puts e.message
	puts e.backtrace.inspect
	puts "-----------------------------------------------------"
	puts "Program exits with the code 1."
	exit 1
else
	puts "The file #{filename} is successfully uploaded to the bucket #{bname}."
end
