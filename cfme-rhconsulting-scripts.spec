Name:      cfme-rhconsulting-scripts
Version:   0.6
Release:   1
Summary:   Red Hat Consulting Scripts for CloudForms

Group:     Applications/System
License:   GPLv3+
URL:       https://github.com/jsimonelli/%{name}
Source:    %{name}-%{version}-%{release}.tar.gz

BuildArch: noarch

%description
These scripts are useful to import/export specific items.

%prep
%autosetup -n %{name}

%build

%install
mkdir -p "%{buildroot}/var/www/miq/vmdb/lib/tasks"
mkdir -p "%{buildroot}/usr/bin"
cd %{_builddir}/%{name}
install --backup --mode=0644 -t "%{buildroot}/var/www/miq/vmdb/lib/tasks/$f" *.rake
install --backup --mode=0755 -t "%{buildroot}/usr/bin" bin/miqexport
install --backup --mode=0755 -t "%{buildroot}/usr/bin" bin/miqimport
install --backup --mode=0755 -t "%{buildroot}/usr/bin" bin/export-miqdomain
install --backup --mode=0755 -t "%{buildroot}/usr/bin" bin/import-miqdomain

%files
/var/www/miq/vmdb/lib/tasks/rhconsulting_buttons.rake
/var/www/miq/vmdb/lib/tasks/rhconsulting_miq_ae_datastore.rake
/var/www/miq/vmdb/lib/tasks/rhconsulting_tags.rake
/var/www/miq/vmdb/lib/tasks/rhconsulting_customization_templates.rake
/var/www/miq/vmdb/lib/tasks/rhconsulting_roles.rake
/var/www/miq/vmdb/lib/tasks/rhconsulting_dialogs.rake
/var/www/miq/vmdb/lib/tasks/rhconsulting_service_catalogs.rake
/var/www/miq/vmdb/lib/tasks/rhconsulting_reports.rake
/var/www/miq/vmdb/lib/tasks/rhconsulting_widgets.rake
/var/www/miq/vmdb/lib/tasks/rhconsulting_policies.rake
/usr/bin/miqexport
/usr/bin/miqimport
/usr/bin/export-miqdomain
/usr/bin/import-miqdomain

%post

%changelog
* Fri Jul 08 2016 Brant Evans <bevans@redhat.com> 0.6
- Added export/import for widgets

* Thu Apr 28 2016 Kumar Jadav <kumar.jadav@redhat.com> 0.5
- Added the import-miqdomain file.

* Tue Apr 26 2016 Kumar Jadav <kumar.jadav@redhat.com> 0.4
- Added the export-miqdomain file.

* Mon Dec 21 2015 George Goh <george.goh@redhat.com> 0.3-2
- Fix wrong test in miqimport for the existence of files.

* Mon Dec 14 2015 Brant Evans <brant.evans@redhat.com> 0.3-1
- Add policy import/export
- Adjust miqexport/miqimport path to fix RPM build errors

* Thu Nov 26 2015 George Goh <george.goh@redhat.com> 0.2-1
- Add custom report import/export
- Add miqimport/miqexport commands

* Wed Aug 26 2015 George Goh <george.goh@redhat.com> 0.1-2
- Bump release version

* Mon Jul 20 2015 George Goh <george.goh@redhat.com> 0.1-1
- Initial RPM release

