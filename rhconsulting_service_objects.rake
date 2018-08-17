require_relative 'rhconsulting_illegal_chars'
require_relative 'rhconsulting_options'

class ServiceObjectImportExport
  class ParsedNonServiceYamlError < StandardError; end

  def export(filedir, options = {})
    raise "Must supply filedir" if filedir.blank?
    service_hash = Service.where(ancestry: nil).order(:id).all
    service_hash.each { |x|
      data = []
      puts "Export Service: #{x['name']}"
      data << export_services(Array.wrap(x)).first()
      # Replace invalid filename characters
      fname = MiqIllegalChars.replace("#{x['name']}-#{x['id']}", options)
      File.write("#{filedir}/#{fname}.yml", data.to_yaml)
    }
  end
  def import(filedir)
    raise "Must supply filedir" if filedir.blank?
    if File.file?(filedir)
      Service.transaction do
        import_services_from_file("#{filedir}")
      end
    elsif File.directory?(filedir)
      Service.transaction do
        Dir.foreach(filedir) do |filename|
          next if filename == '.' or filename == '..'
          import_services_from_file("#{filedir}/#{filename}")
        end
      end
    end
  end

  private

  # === BEGIN Export ===============================================
  def export_services(services)
    services.map do |svc|
      obj = {}
      obj.merge!(included_attributes(svc.attributes, ["updated_at"]))
      obj.merge!(translate_ids(svc))
      obj['custom_attributes'] = included_collections(svc.custom_attributes, ['id', 'resource_id', 'value_interpolated', 'serialized_value', 'unique_name'])
      obj['tags']              = included_collections(svc.tags, ['id'])
      obj['vms']               = export_vms(svc)
      obj['children']          = export_services(svc.direct_service_children())

      # Transformations
      #
      # Set the Service ID as custom Aggribute
      obj['custom_attributes'] = overwrite_custom_atribute(obj['custom_attributes'], {'name'=>'Import ID:', 'value' => svc.id.to_s}, :add_missing)

      # set custom_1 to "<name> (Owned by <RequesterDepartment>)"
      cust_attr_req_department = obj['custom_attributes'].select { |attr| attr['name'] == 'Requester Department:' }.first()
      custom_label = "#{svc['name']} (Owned by department: #{cust_attr_req_department.fetch('value', 'Not found!')})" unless cust_attr_req_department.blank?
      obj['custom_attributes'] = overwrite_custom_atribute(obj['custom_attributes'], {'name'=>'custom_1', 'value' => custom_label}) unless custom_label.blank?

      # rename service_catalog
      obj['custom_attributes'] = overwrite_custom_atribute(obj['custom_attributes'], {'name'=>'Catalog Item', 'value' => 'Loadbalancer - New Instance'}, :add_missing)           if obj['service_template']['name'] == 'Create Loadbalancer vHost'
      obj['custom_attributes'] = overwrite_custom_atribute(obj['custom_attributes'], {'name'=>'Catalog Item', 'value' => 'MSSQL Database Instance (managed)'}, :add_missing)     if obj['service_template']['name'] == 'MSSQL Database'
      obj['custom_attributes'] = overwrite_custom_atribute(obj['custom_attributes'], {'name'=>'Catalog Item', 'value' => 'SSL Certificate'}, :add_missing)                       if obj['service_template']['name'] == 'New Certificate'
      obj['custom_attributes'] = overwrite_custom_atribute(obj['custom_attributes'], {'name'=>'Catalog Item', 'value' => 'VM Linux (managed) - standard options'}, :add_missing) if obj['service_template']['name'] == 'VM Linux (managed)'
      obj['custom_attributes'] = overwrite_custom_atribute(obj['custom_attributes'], {'name'=>'Catalog Item', 'value' => 'VM Windows Server 2012 (managed)'}, :add_missing)      if obj['service_template']['name'] == 'VM Windows (managed)'
      obj['custom_attributes'] = overwrite_custom_atribute(obj['custom_attributes'], {'name'=>'Catalog Item', 'value' => 'VM Linux Openshift Node'}, :add_missing)               if obj['service_template']['name'] == 'OpenShift ready RHEL7 VM'

      obj
    end
  end
  def translate_ids(svc)
    attrs = {}
    attrs['parent'] = included_attributes(svc.parent.attributes, ['updated_at']) unless svc.parent == nil
    attrs['service_template'] = included_attributes(svc.service_template.attributes, ['id', 'guid', 'options', 'updated_at']) unless svc.service_template == nil
    attrs['evm_owner'] = included_attributes(svc.evm_owner.attributes, ['id']) unless svc.evm_owner == nil
    attrs['miq_group'] = included_attributes(svc.miq_group.attributes, ['id', 'updated_on']) unless svc.miq_group == nil
    attrs
  end
  def overwrite_custom_atribute(existing_attributes, attribute, strategy=:overwrite)
    id_index = existing_attributes.find_index {|attr| attr['name'] == attribute['name'] }
    if id_index == nil
      existing_attributes.concat(Array.wrap(attribute))
    else
      if strategy == :overwrite
        existing_attributes[id_index].merge!(attribute)
       else
        existing_attributes[id_index].reverse_merge!(attribute)
       end
    end
    existing_attributes
  end
  def export_vms(svc)
    svc.direct_vms.map do |vm|
      vm_hash = {}
      vm_hash['ems_ref'] = vm.ems_ref
      vm_hash['name'] = vm.name
      vm_hash
    end
  end

  # === END Export =================================================

  # === BEGIN Import ===============================================
  def import_services_from_file(filename)
    services = YAML.load_file(filename)
    import_services(nil, services)
  end
  def import_services(parent, services)
    begin
      services.each do |svc|
        #puts "Service: [#{svc['name']}]: #{find_service(svc).length}"

        if find_service(svc).empty?
          puts "NEW    Service #{svc['name']}"
          import_single_service(parent, svc)
          import_services(svc, svc.fetch('children', []))
        else
          puts "EXISTS Service #{svc['name']}"
          import_services(svc, svc.fetch('children', []))
        end
      end
    rescue
      raise ParsedNonServiceYamlError
    end
  end
  def import_single_service(parent, svc)
    puts "Creating Service #{svc['name']}"

    new_svc = Service.create!(get_service_hash(svc))
    raise 'Service not created' if new_svc.blank?
    ae_service = get_ae_service(new_svc)
    assign_tags(new_svc, svc)
    set_custom_attributes(new_svc, svc, ['Catalog Item'])

    parent_obj = find_parent_service(parent, svc) unless parent.blank?
    puts "    Parent   : #{parent_obj.name}" unless parent_obj.blank?
    ae_service.parent_service = get_ae_service(parent_obj) unless parent_obj.blank?

    vms = find_vms(svc).reject {|vm| vm.blank? }
    puts "    VMs      : #{vms.map {|vm| vm.name }.join(' ')}" unless vms.blank?
    vms.each {|vm| vm.remove_from_service() if vm.service.present? }
    vms.each {|vm| vm.add_to_service(ae_service) }
  end
  def get_ae_service(svc)
    return MiqAeMethodService::MiqAeServiceService.where(id: svc.id).first
  end
  def get_service_hash(svc)
    service_hash = {}
    service_hash['name']                 = svc['name']
    service_hash['description']          = svc['description']
    service_hash['display']              = svc['display']
    service_hash['miq_group_id']         = get_miq_group_id(svc)
    service_hash['type']                 = svc['type'] unless svc['type'].blank?
    service_hash['options']              = {}
    service_hash['options']['dialog']    = svc.fetch_path(['options', 'dialog'])
    service_hash['evm_owner_id']         = get_evm_owner_id(svc)
    service_hash['retired']              = svc['retired']
    service_hash['retires_on']           = svc['retires_on']
    service_hash['retirement_warn']      = svc['retirement_warn']
    service_hash['retirement_last_warn'] = svc['retirement_last_warn']
    service_hash['retirement_state']     = svc['retirement_state']
    service_hash['retirement_requester'] = svc['retirement_requester']
    service_hash['service_template_id']  = get_service_template_id(svc)
#    service_hash['']                     = svc['']
    service_hash
  end
  def find_parent_service(parent, svc)
    existing_parents = find_service(parent)
    if existing_parents.length == 1
      return existing_parents.first
    elsif existing_parents.length > 1
      puts "    Ambigiuos parent service"
      return nil
    elsif existing_parents.length == 0
      puts "    Parent service not found"
      return nil
    end
  end
  def find_vms(svc)
    svc.fetch('vms', []).map {|vm|
      #puts "Searching for VM #{vm}"
      MiqAeMethodService::MiqAeServiceVm.where(ems_ref: vm['ems_ref']).first()
    }
  end
  def assign_tags(new_svc, svc)
    # Create if not already present
    tags=svc['tags']
    tags.each do |t|
      tag = Tag.find_by_name(t['name'])
      if tag.blank?
        puts "    Tag      : #{t['name']} does not exist"
      else
        puts "    Tag      : #{t['name']} has id #{tag.id}"
        Classification.classify_by_tag(new_svc, tag.name)
      end
    end
  end
  def set_custom_attributes(new_svc, svc, ignored_attrs = [])
    # set custom_attributes
    attrs = svc['custom_attributes']
    # do not import ignored_attrs, except Service IDs of parent Services
    attrs.reject!{ |a| ignored_attrs.include?(a['name']) }
    attrs.each do |attr|
      puts "    Cust Attr: #{attr['name']} = #{attr['value']}"
      new_svc.miq_custom_set(attr['name'], attr['value'])
    end
  end
  def get_miq_group_id(svc)
    cust_attr_req_department = svc['custom_attributes'].select { |attr| attr['name'] == 'Requester Department:' }.first()
    group_name = cust_attr_req_department.fetch('value', nil) unless cust_attr_req_department.blank?
    group_name = svc.fetch_path(['miq_group', 'description']) if group_name.blank?
    group_name = 'EvmGroup-super_administrator' if group_name.blank?

    miq_groups = MiqGroup.where(description: group_name)

    if miq_groups.length == 1
      return miq_groups.first.id
    elsif miq_groups.length == 0
      puts "    No group matched! Defaulting to 'EvmGroup-super_administrator'"
      return MiqGroup.where(description: 'EvmGroup-super_administrator').first.id
    else
      puts "    More than one group matched! Defaulting to 'EvmGroup-super_administrator'"
      return MiqGroup.where(description: 'EvmGroup-super_administrator').first.id
    end
  end
  def get_evm_owner_id(svc)
    owner_name = svc.fetch_path(['evm_owner','userid'])
    return nil if owner_name.blank?

    users = User.where(userid: owner_name)
    if users.length == 1
      return users.first.id
    else
      puts "    User '#{owner_name}' not found!"
      return nil
    end
  end
  def get_service_template_id(svc)
    cust_attr = svc.fetch_path(['custom_attributes']).select{|attr| attr['name'] == 'Catalog Item' }.first()
    service_template_name = cust_attr['value'] if cust_attr.present?
    service_template_name = svc.fetch_path(['service_template','name']) if service_template_name.blank?
    return nil if service_template_name.blank?

    service_templates = ServiceTemplate.where(name: service_template_name)
    if service_templates.length == 1
      return service_templates.first.id
    else
      puts "    Service Template '#{service_template_name}' not found!"
      return nil
    end
  end
  # === END Import =================================================


  # === BEGIN Helper ===============================================
  def included_attributes(attributes, excluded_attributes)
    attributes.reject { |key, _| excluded_attributes.include?(key) }
  end
  def included_collections(active_record, excluded_attributes)
    active_record.map { |record| included_attributes(record.attributes, excluded_attributes) }
  end

  def find_service(svc)
    attr_name = 'Import ID:'
    duplicates = Service.where(name: svc['name'])
    import_id = svc['custom_attributes'].select {|attr| attr['name'] == attr_name }.first.fetch('value', nil)
#    import_id = svc.fetch('id', nil) unless import_id.present?

    duplicates.select do |svc|
      existing_id = svc.miq_custom_get(attr_name) if svc.miq_custom_keys().include?(attr_name)
      existing_id = svc.id if existing_id.blank?
      existing_id.to_s == import_id.to_s
    end
  end
#  def find_service_without_id(svc)
#    # Check if with that name/description is already in the database?
#    # if not --> new service
#    duplicates = Service.where(name: svc['name'], description: svc['description'])
#    puts "No matching Service in DB" if duplicates.empty?
#    return nil if duplicates.empty?
#
#    #
#    duplicates = duplicates.select do |s|
#      (s.parent == nil and svc.fetch_path(['parent','name']) == nil) or (s.parent.name == svc.fetch_path(['parent','name']) and s.parent.description == svc.fetch_path(['parent','description']) )
#    end
#    puts "Not the same parent" if duplicates.empty?
#    return nil if duplicates.empty?
#
#
#    check_attrs = [['miq_group','description'], ['service_template','name'], ['service_template','description']]
#    duplicates = duplicates.select do |s|
#      check_attrs.all? do |path|
#        to_import =  svc.fetch_path(path)
#
#        rails_model = s.send(path[0].to_sym).send(path[1].to_sym) if s.send(path[0].to_sym).present?
#        rails_model == to_import
#      end
#    end
#    puts "Something else differs" if duplicates.empty?
#    return nil if duplicates.empty?
#
#   return duplicates
#  end
end

namespace :rhconsulting do
  namespace :service_objects do

    desc 'Usage information'
    task :usage => [:environment] do
      puts 'Export - Usage: rake rhconsulting:service_objects:export[/path/to/dir/with/services]'
      puts 'Import - Usage: rake rhconsulting:service_objects:import[/path/to/dir/with/services]'
    end

    desc 'Import all service dialogs to individual YAML files'
    task :import, [:filedir] => [:environment] do |_, arguments|
      ServiceObjectImportExport.new.import(arguments[:filedir])
    end

    desc 'Exports all service dialogs to individual YAML files'
    task :export, [:filedir] => [:environment] do |_, arguments|
      options = RhconsultingOptions.parse_options(arguments.extras)
      ServiceObjectImportExport.new.export(arguments[:filedir], options)
    end

  end
end
