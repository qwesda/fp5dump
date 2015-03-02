from collections import OrderedDict
import sys
import logging
import time
import locale
import parsedatetime as pdt

from .blockchain import decode_vli


class Exporter(object):
    def __init__(self, fp5file, export_definition,
                 first_record_to_process=None, table_name=None, show_progress=False, drop_empty_columns=False):

        super(Exporter, self).__init__()

        self.fp5file = fp5file
        self.export_definition = export_definition
        self.first_record_to_process = first_record_to_process
        self.table_name = table_name
        self.show_progress = show_progress
        self.drop_empty_columns = drop_empty_columns

        self.logging = logging.getLogger('fp5dump.fp5file.fp5file')

        self.start_time = None
        self.eta_last_updated = None

        self.ptd_parser = None

        self.processed_records = 0
        self.inserted_records = 0
        self.updated_records = 0
        self.deleted_records = 0

        self.failed_records = 0
        self.records_to_process_count = 0

        self.sampled_errors_for_fields = OrderedDict()

        self.decimal_point_char = b'.'[0]
        self.thousands_separator_char = b','[0]

    def set_locale(self):
        if not self.fp5file.locale:
            time_locale = locale.getlocale(locale.LC_TIME)
            self.fp5file.locale = locale.getlocale(locale.LC_NUMERIC)
        else:
            time_locale = self.fp5file.locale

        try:
            locale.setlocale(locale.LC_NUMERIC, self.fp5file.locale)

            localeconv = locale.localeconv()

            if localeconv['decimal_point'] and len(localeconv['decimal_point']) == 1:
                self.decimal_point_char = ord(localeconv['decimal_point'])

            if localeconv['thousands_sep'] and len(localeconv['thousands_sep']) == 1:
                self.thousands_separator_char = ord(localeconv['thousands_sep'])
            else:
                self.thousands_separator_char = None

            self.ptd_parser = pdt.Calendar(pdt.Constants(time_locale))
        except locale.Error:
            self.logging.warn("could not set locale to '%s'" % self.fp5file.locale)

    @staticmethod
    def reset_locale():
        locale.resetlocale()

    def aggregate_errors(self, field_id_bin, record_id, error_value):
        if field_id_bin not in self.sampled_errors_for_fields:
            self.sampled_errors_for_fields[field_id_bin] = OrderedDict()

        if len(self.sampled_errors_for_fields[field_id_bin]) < 100:
            self.sampled_errors_for_fields[field_id_bin][record_id] = error_value

    def format_errors(self):
        error_texts = []

        if self.sampled_errors_for_fields:
            error_texts.append("errors for tables: '%s'" % self.table_name)

            for field_id_bin, sampled_errors in self.sampled_errors_for_fields.items():
                field_def = self.export_definition[field_id_bin]

                error_texts.append("\n%d%s errors for field '%s' (%s):" % (len(sampled_errors),
                                                                           "+" if len(sampled_errors) == 100 else "",
                                                                           field_def.field.label,
                                                                           field_def.type))


                for record_id, error_value in sampled_errors.items():
                    error_texts.append("%s \t %s" % (record_id, error_value))

        return "\n".join(error_texts)

    def update_progress(self):
        self.processed_records += 1

        now = time.time()

        if now - self.eta_last_updated >= 1:
            self.eta_last_updated = now

            padding = len(str(self.records_to_process_count))

            seconds_elapsed = self.eta_last_updated - self.start_time
            seconds_remaining = (self.records_to_process_count - self.processed_records) * (seconds_elapsed / self.processed_records)

            eta_string = " ETA: %d:%02d" % (seconds_remaining // 60, seconds_remaining % 60)
            elapsed_string = " %d:%02d" % (seconds_elapsed // 60, seconds_elapsed % 60)

            format_string = "%%%dd/%%d" % padding
            progress_info = format_string % (self.processed_records, self.records_to_process_count)

            progress_info += elapsed_string + eta_string + "    "

            if self.processed_records < self.records_to_process_count:
                sys.stdout.write(progress_info)
                sys.stdout.flush()
                sys.stdout.write('\b' * len(progress_info))
            else:
                sys.stdout.flush()