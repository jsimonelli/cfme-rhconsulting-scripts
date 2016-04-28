VERSION := 0.5
RELEASE := 2

.PHONY: clean rpm install clean-install

rm-installed-files:
	rm -f /var/www/miq/vmdb/lib/tasks/rhconsulting_buttons.rake
	rm -f /var/www/miq/vmdb/lib/tasks/rhconsulting_customization_templates.rake
	rm -f /var/www/miq/vmdb/lib/tasks/rhconsulting_dialogs.rake
	rm -f /var/www/miq/vmdb/lib/tasks/rhconsulting_miq_ae_datastore.rake
	rm -f /var/www/miq/vmdb/lib/tasks/rhconsulting_roles.rake
	rm -f /var/www/miq/vmdb/lib/tasks/rhconsulting_service_catalogs.rake
	rm -f /var/www/miq/vmdb/lib/tasks/rhconsulting_tags.rake
	rm -f /var/www/miq/vmdb/lib/tasks/rhconsulting_reports.rake
	rm -f /var/www/miq/vmdb/lib/tasks/rhconsulting_policies.rake
	rm -f /usr/bin/miqexport
	rm -f /usr/bin/miqimport
	rm -f /usr/bin/export-miqdomain
	rm -f /usr/bin/import-miqdomain

install:
	install -Dm644 rhconsulting_buttons.rake /var/www/miq/vmdb/lib/tasks/rhconsulting_buttons.rake
	install -Dm644 rhconsulting_customization_templates.rake /var/www/miq/vmdb/lib/tasks/rhconsulting_customization_templates.rake
	install -Dm644 rhconsulting_dialogs.rake /var/www/miq/vmdb/lib/tasks/rhconsulting_dialogs.rake
	install -Dm644 rhconsulting_miq_ae_datastore.rake /var/www/miq/vmdb/lib/tasks/rhconsulting_miq_ae_datastore.rake
	install -Dm644 rhconsulting_roles.rake /var/www/miq/vmdb/lib/tasks/rhconsulting_roles.rake
	install -Dm644 rhconsulting_service_catalogs.rake /var/www/miq/vmdb/lib/tasks/rhconsulting_service_catalogs.rake
	install -Dm644 rhconsulting_tags.rake /var/www/miq/vmdb/lib/tasks/rhconsulting_tags.rake
	install -Dm644 rhconsulting_reports.rake /var/www/miq/vmdb/lib/tasks/rhconsulting_reports.rake
	install -Dm644 rhconsulting_policies.rake /var/www/miq/vmdb/lib/tasks/rhconsulting_policies.rake
	install -Dm755 bin/miqexport /usr/bin/miqexport
	install -Dm755 bin/miqimport /usr/bin/miqimport
	install -Dm755 bin/export-miqdomain /usr/bin/export-miqdomain
	install -Dm755 bin/import-miqdomain /usr/bin/import-miqdomain

clean-install: rm-installed-files install


TARBALL := $(shell rpmbuild -E %{_sourcedir})/cfme-rhconsulting-scripts-$(VERSION)-$(RELEASE).tar.gz

rpm:
	rm -rf cfme-consulting-scripts && \
	mkdir -p cfme-rhconsulting-scripts/bin && \
	cp *.rake cfme-rhconsulting-scripts && \
	cp bin/* cfme-rhconsulting-scripts/bin && \
	tar zcf "$(TARBALL)" cfme-rhconsulting-scripts && \
	rm -rf cfme-rhconsulting-scripts && \
	rpmbuild -bb cfme-rhconsulting-scripts.spec

clean:
	rm -rf cfme-consulting-scripts
	rm -f "$(TARBALL)"
