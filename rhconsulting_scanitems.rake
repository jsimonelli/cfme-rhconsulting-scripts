# Author: A Liu Ly <alejandrol@t-systems.com>
#require_relative 'rhconsulting_illegal_chars'
require_relative 'rhconsulting_options'

class MiqScanItemsImportExport
  class ParsedNonDialogYamlError < StandardError; end

  def export(export_dir, options = {})
    raise "Must supply export dir" if export_dir.blank?

    # Export the Scan Items
    export_scan_items(export_dir, options)

  end

  def import(import_dir)
    raise "Must supply import dir" if import_dir.blank?

    # Import the Scan Items
    import_scan_items(import_dir)
  end

private

  def export_scan_items(export_dir, options)
    ScanItemSet.all.each do |p|
      # Skip read only entries
      if p.read_only 
        next
      end
      # Also skip any entries that are using file defaults...
      uses_files = false
      p.members.each do |m|
        if m.filename
          uses_files = true
        end
      end
      if uses_files
        next
      end

      
      puts("Exporting Scan Profile: #{p.name} (#{p.description}) (#{p.guid})")

      fname = "ScanProfile_#{p.guid}.yaml"

      # Clean-up data
      profile = ScanItem.get_profile(p.name).first.dup
      %w(id created_on updated_on).each { |k| profile.delete(k) }
      profile['definition'].each do |dd|
        %w(id created_on updated_on description).each { |k| dd.delete(k) }
      end
      
      s = profile.to_yaml
      s.gsub!(/\n\s*guid:\s+\S+\n/,"\n")
      File.write("#{export_dir}/#{fname}", s)
    end
  end

  def import_scan_items(import_dir)
    Dir.glob("#{import_dir}/ScanProfile_*yaml") do |filename|
      puts("Importing Scan Profile: #{File.basename(filename, '.yaml').gsub(/^ScanProfile_/, '')} ....")

      hash = YAML.load_file(filename)
      items = hash["definition"]
      hash.delete("definition")
      profile = ScanItemSet.find_by(:name => hash["name"]);
      if profile.nil?
        if hash["guid"].nil?
          hash["guid"] = SecureRandom.uuid
        end
        profile = ScanItemSet.new(hash)
      else
        profile.attributes = hash
      end
      profile.save!
      
      # Delete existing members
      profile.members.each do |one|
        if one.filename
          # OK, this was defined through yaml file... just skip it
          next
          #profile.delete(one)
        else
          one.destroy
        end
      end
      items.each do |i|
        if i['filename']
          # OK, this rules refers to a file, just use it...
          next
        else
          if i['guid'].nil?
            i['guid'] = SecureRandom.uuid
          end
          #puts i.inspect
          si = ScanItem.create(i)
        end
        profile.add_member(si)
      end
    end
  end

end

namespace :rhconsulting do
  namespace :miq_scanprofiles do

    desc 'Usage information'
    task :usage => [:environment] do
      puts 'Export - Usage: rake \'rhconsulting:miq_scanprofiles:export[/path/to/dir/with/scan/items]\''
      puts 'Import - Usage: rake \'rhconsulting:miq_scanprofiles:import[/path/to/dir/with/scan/items]\''
    end

    desc 'Exports all scan profiles to individual YAML files'
    task :export, [:filedir] => [:environment] do |_, arguments|
      options = RhconsultingOptions.parse_options(arguments.extras)
      MiqScanItemsImportExport.new.export(arguments[:filedir], options)
    end

    desc 'Imports all scan profiles from individual YAML files'
    task :import, [:filedir] => [:environment] do |_, arguments|
      MiqScanItemsImportExport.new.import(arguments[:filedir])
    end

  end
end
