# Author: A Liu Ly <alejandrol@t-systems.com>
require_relative 'rhconsulting_illegal_chars'
require_relative 'rhconsulting_options'

class MiqScrSrcImportExport
  class ParsedNonDialogYamlError < StandardError; end

  def export(export_dir, options = {})
    raise "Must supply export dir" if export_dir.blank?
    export_repo_items(export_dir, options)
  end

  def import(import_dir)
    raise "Must supply import dir" if import_dir.blank?
    # Import the git repos
    import_repo_items(import_dir)
  end

private
  def extract_scrsrc(src)
    gitrepo = {
      "name" => src.name,
      "type" => src.type,
      "description" => src.description,
      "scm_type" => src.scm_type,
      "scm_url" => src.scm_url,
      "scm_branch" => src.scm_branch,
      "scm_clean" => src.scm_clean,
      "scm_delete_on_update" => src.scm_delete_on_update,
      "scm_update_on_launch" => src.scm_update_on_launch,
    }
    if ! src.authentication_id.nil?
      gitrepo["authentication"] = ::Authentication.find_by(:id => src.authentication_id).name
    end
    return gitrepo
  end

  def lookup_auth(gitrepo)
    if gitrepo.has_key?("authentication")
      gitrepo["authentication_id"] = ::Authentication.find_by(:name => gitrepo["authentication"]).id
      gitrepo.delete("authentication")
    end
  end

  def export_repo_items(export_dir, options)
    ManageIQ::Providers::EmbeddedAnsible::AutomationManager::ConfigurationScriptSource.all.each do |src|

      pname = src['id']
      fname = "#{pname}.yaml"
      puts("Export: #{src.name} to #{fname}")
      repo = extract_scrsrc(src)
      File.write("#{export_dir}/#{fname}", repo.to_yaml)
    end
  end

  def import_repo_items(import_dir)
    ea = Provider.find_by(:type => "ManageIQ::Providers::EmbeddedAnsible::Provider")
    return if ea.nil?

    Dir.glob("#{import_dir}/*.yaml") do |filename|
      puts("Importing Repo File: #{File.basename(filename, '.yaml')} ....")
      yaml = YAML.load_file(filename)
      csrc = ManageIQ::Providers::EmbeddedAnsible::AutomationManager::ConfigurationScriptSource.find_by(:name => yaml["name"])
      if csrc.nil?
	# Create repo
	puts("Creating repo #{yaml["name"]}")
	lookup_auth(yaml)
	ManageIQ::Providers::EmbeddedAnsible::AutomationManager::ConfigurationScriptSource.create_in_provider_queue(ea.id, yaml.deep_symbolize_keys)

	# Check that the script source was created
	retries = 10
	begin
	  retries -= 1
	  csrc = ManageIQ::Providers::EmbeddedAnsible::AutomationManager::ConfigurationScriptSource.find_by(:name => yaml["name"])
	  if ! csrc.nil?
	    puts "Created #{yaml['name']} as #{csrc.id}"
	    break
	  end      
	  sleep 6
	end while retries > 0

      else
	tm = extract_scrsrc(csrc)
	if tm.to_yaml != yaml.to_yaml
	  puts("Modify #{yaml["name"]}")
	  yy = yaml.dup
	  lookup_auth(yy)
	  #puts(yy.to_yaml)
	  csrc.update_in_provider_queue(yy.deep_symbolize_keys)
	  # Check that the script source was updated
	  retries = 10
	  begin
	    retries -= 1
	    csrc = ManageIQ::Providers::EmbeddedAnsible::AutomationManager::ConfigurationScriptSource.find_by(:name => yaml["name"])
	    if ! csrc.nil?
	      tm = extract_scrsrc(csrc)
	      if tm.to_yaml == yaml.to_yaml
		puts "Updated #{yaml['name']} as #{csrc.id}"
		break
	      end
	    end      
	    sleep 6
	  end while retries > 0
	end
      end
    end
    ManageIQ::Providers::EmbeddedAnsible::AutomationManager.refresh_ems([ea.id])
  end

end

namespace :rhconsulting do
  namespace :miq_scriptsrc do

    desc 'Usage information'
    task :usage => [:environment] do
      puts 'Import - Usage: rake \'rhconsulting:miq_scriptsrc:import[/path/to/dir/with/script/sources]\''
      puts 'Export - Usage: rake \'rhconsulting:miq_scriptsrc:export[/path/to/dir/with/script/sources]\''
    end

    desc 'Imports all script sources from individual YAML files'
    task :import, [:filedir] => [:environment] do |_, arguments|
      MiqScrSrcImportExport.new.import(arguments[:filedir])
    end
    desc 'Exports all script sources to individual YAML files'
    task :export, [:filedir] => [:environment] do |_, arguments|
      MiqScrSrcImportExport.new.export(arguments[:filedir])
    end

  end
end
