# Author: Dustin Scott <dscott@redhat.com>
#require_relative 'rhconsulting_illegal_chars'
require_relative 'rhconsulting_options'
require_relative 'rhconsulting_model_attributes'

class MiqSchedulesImportExport
  class ParsedNonDialogYamlError < StandardError; end

  attr_reader :invalid_schedules, :rejected_attrs, :file_depot_key, :zone_key, :role_types

  def initialize
    @file_depot_key    = :file_depot_export_data        # the key used to store exported file depot data
    @zone_key          = :zone_export_data              # the key used to store exported zone data
    @invalid_schedules = [
      'MiqAlert',
      'MiqReport',
      'MiqWidget'
    ]
    @rejected_attrs    = [                              # attributes to leave out during export
      :file_depot_id,
      :miq_schedule_id,
      :miq_search_id,
      :last_run_on,
      :task_id,
      :zone_id
    ]
    @role_types        = {                              # mapping of schedule types to their required roles
      'AutomationRequest' => 'automate',
      'DatabaseBackup'    => 'database_operations',
      'EmsCluster'        => 'smartstate',
      'MiqAlert'          => 'notifier',
      'MiqReport'         => 'reporting',
      'MiqWidget'         => 'reporting',
      'Host'              => 'smartstate',
      'Vm'                => 'smartstate'
    }
  end

  def export(export_dir, options = {})
    raise "Must supply export dir" if export_dir.blank?

    # export the schedules
    export_schedules(export_dir, options)
  end

  def import(import_dir)
    raise "Must supply import dir" if import_dir.blank?

    # import the schedules
    import_schedules(import_dir)
  end

private

  # get the file depot in the following order:
  #   file depot by id:  generally available during export
  #   file depot by uri: generally available during import, after export
  def export_file_depot(model_object_attrs)
    return nil unless model_object_attrs[:towhat] == 'DatabaseBackup'

    selected_file_depot   = nil
    selected_file_depot ||= FileDepot.find_by(:id  => model_object_attrs[:file_depot_id])
    selected_file_depot ||= FileDepot.find_by(:uri => model_object_attrs[@file_depot_key].try(:fetch, :uri))
  end

  # get the zone in the following order:
  #   zone by id:   generally available during export
  #   zone by name: generally available during import, after export
  #   zone by role: available during import, the first zone which has the appropriate role based on schedule type
  #   default zone: use the default zone (generally the first available) if no others are available
  def export_zone(model_object_attrs)
    selected_role   = @role_types.fetch(model_object_attrs[:towhat])
    selected_zone   = nil
    selected_zone ||= Zone.find_by(:id   => model_object_attrs[:zone_id])
    selected_zone ||= Zone.find_by(:name => model_object_attrs[@zone_key].try(:fetch, :name))
    selected_zone ||= Zone.all.select { |zone| zone.role_assigned?(selected_role) }.first
    selected_zone ||= Zone.first
  end

  # get the user in the following order:
  #   user by userid: generally available during export
  #   default user:   use the admin user in the absence of the user assigned by the export data 
  def export_user(model_object_attrs)
    User.find_by(:userid => model_object_attrs[:userid]) || User.find_by(:userid => 'admin')
  end

  # do not manipulate the model_object_attrs unless we have a file depot attached
  # not all schedules will have a file depot
  def add_file_depot_attrs(model_object_attrs)
    depot_attrs = export_file_depot(model_object_attrs).try(:attributes)
    return model_object_attrs unless depot_attrs

    parsed_depot_attrs = {
      @file_depot_key => RhconsultingModelAttributes.parse_attributes(depot_attrs, :support_case)
    }
    model_object_attrs.merge!(parsed_depot_attrs)
  end

  # add zone attributes to the export so that we know how to search for it during import
  # e.g. the zone_id does us no good because they are likely to be different
  def add_zone_attrs(model_object_attrs)
    zone       = export_zone(model_object_attrs)
    zone_name  = zone ? zone.name : 'default'
    zone_attrs = {
      @zone_key => {
        :name => zone_name
      }
    }

    model_object_attrs.merge!(zone_attrs)
  end

  # normalize the following so that it is ready to be imported:
  #   symbolize_keys: provide a consistent hash with symbols as keys
  #   file_depot:     needed for import when DatabaseBackup type
  #   zone:           name needed for import
  #   options:        remove the options hash as all of the necessary data livees in the body
  #                   and this is very inconsistent across schedule types (e.g. db vs. widget)
  def normalize_export_data(raw_attrs)
    raw_attrs_hash = raw_attrs.deep_symbolize_keys
    add_file_depot_attrs(raw_attrs_hash) if raw_attrs_hash[:towhat] == 'DatabaseBackup'
    add_zone_attrs(raw_attrs_hash)
    raw_attrs_hash[:sched_action].delete(:options)
    RhconsultingModelAttributes.parse_attributes(raw_attrs_hash, @rejected_attrs)
  end

  def export_schedules(export_dir, options)
    MiqSchedule.all.each do |schedule|
      # skip the schedule if it is invalid
      next if @invalid_schedules.include?(schedule.towhat)

      # normalize the attributes from the model
      normalized_attrs = normalize_export_data(schedule.attributes)

      # set the filename and replace spaces and characters that are not allowed in filenames
      fname = "#{schedule.id}.yaml"
      File.write("#{export_dir}/#{fname}", normalized_attrs.to_yaml)
    end
  end

  def normalize_import_data(raw_attrs)
    user = export_user(raw_attrs)
    zone = export_zone(raw_attrs)

    # find and create the file depot if needed
    if raw_attrs[:towhat] == 'DatabaseBackup'
      file_depot   = export_file_depot(raw_attrs)
      file_depot ||= FileDepot.create(raw_attrs[@file_depot_key])
      raw_attrs.merge!(:file_depot_id => file_depot.id)
    end

    # update the options
    raw_attrs.merge!(:userid => user.userid, :zone_id => zone.id)

    # delete the additional data that our model does not need
    [@file_depot_key, @zone_key].each { |key| raw_attrs.delete(key) }

    # return the newly modified attrs
    return raw_attrs
  end

  def import_schedules(import_dir)
    Dir.glob("#{import_dir}/*yaml") do |filename|
      MiqSchedule.transaction do
        schedule            = YAML.load_file(filename)
        normalized_schedule = normalize_import_data(schedule)
        MiqSchedule.create(normalized_schedule)
      end
    end
  end
end

namespace :rhconsulting do
  namespace :miq_schedules do
    desc 'Usage information'
    task :usage => [:environment] do
      puts 'Export - Usage: rake \'rhconsulting:miq_schedules:export[/path/to/dir/with/widgets]\''
      puts 'Import - Usage: rake \'rhconsulting:miq_schedules:import[/path/to/dir/with/widgets]\''
    end

    desc 'Exports all schedules (non-widget type) to individual YAML files'
    task :export, [:filedir] => [:environment] do |_, arguments|
      options = RhconsultingOptions.parse_options(arguments.extras)
      MiqSchedulesImportExport.new.export(arguments[:filedir], options)
    end

    desc 'Imports all schedules (non-widget type) from individual YAML files'
    task :import, [:filedir] => [:environment] do |_, arguments|
      MiqSchedulesImportExport.new.import(arguments[:filedir])
    end
  end
end
