import sys
import logging
import time
import locale
import parsedatetime as pdt


class Exporter(object):
    def __init__(self, fp5file, export_definition,
                 first_record_to_export=None, table_name=None, show_progress=False, drop_empty_columns=False):

        super(Exporter, self).__init__()

        self.fp5file = fp5file
        self.export_definition = export_definition
        self.first_record_to_export = first_record_to_export
        self.table_name = table_name
        self.show_progress = show_progress
        self.drop_empty_columns = drop_empty_columns

        self.logging = logging.getLogger('fp5dump.fp5file.fp5file')

        self.start_time = None
        self.eta_last_updated = None

        self.ptd_parser = None

        self.exported_records = 0
        self.failed_records = 0
        self.records_to_export_count = 0

        self.sampled_errors_for_fields = {}

    def set_locale(self):
        if not self.fp5file.locale:
            time_locale = locale.getlocale(locale.LC_TIME)
            self.fp5file.locale = locale.getlocale(locale.LC_NUMERIC)
        else:
            time_locale = self.fp5file.locale

        try:
            locale.setlocale(locale.LC_NUMERIC, self.fp5file.locale)
            self.ptd_parser = pdt.Calendar(pdt.Constants(time_locale))
        except locale.Error:
            self.logging.warn("could not set locale to '%s'" % self.fp5file.locale)

    def reset_locale(self):
        locale.resetlocale()

    def aggregate_errors(self, values, field_def, error_value, batch_fields_present):
        error_report_column_values = []

        for error_report_field_def in self.fp5file.error_report_columns:
            if error_report_field_def.field_id in batch_fields_present:
                error_report_column_values.append(values[batch_fields_present.index(error_report_field_def.field_id)])


        if field_def.field_id not in self.sampled_errors_for_fields:
             self.sampled_errors_for_fields[field_def.field_id] = []

        if len(self.sampled_errors_for_fields[field_def.field_id]) < 100:
            if field_def in self.fp5file.error_report_columns:
                error_report_column_values[self.fp5file.error_report_columns.index(field_def)] = error_value
            else:
                error_report_column_values.append(error_value)

            self.sampled_errors_for_fields[field_def.field_id].append(error_report_column_values)

    def format_errors(self):
        error_texts = []

        if self.sampled_errors_for_fields:
            error_texts.append("errors for tables: '%s'" % self.table_name)

            for error_field_id, error_infos in self.sampled_errors_for_fields.items():
                for export_def in self.export_definition.values():
                    if error_field_id == export_def.field.id:
                        error_texts.append("\n%d%s errors for field '%s' (%s):" % (len(error_infos),
                                                                                   "+" if len(error_infos) == 100 else "",
                                                                                   export_def.field.label,
                                                                                   export_def.type))

                        break

                for error_info in error_infos:
                    error_texts.append(str(error_info))

        return "\n".join(error_texts)

    def show_progress_info(self):
        padding = len(str(self.records_to_export_count))

        eta_last_updated = time.time()

        seconds_elapsed = eta_last_updated - self.start_time
        seconds_remaining = (self.records_to_export_count - self.exported_records) * (
            seconds_elapsed / self.exported_records)
        eta_string = " ETA: %d:%02d " % (seconds_remaining // 60, seconds_remaining % 60)

        format_string = "%%%dd/%%d" % padding
        progress_info = format_string % (self.exported_records, self.records_to_export_count)

        progress_info += eta_string

        if self.exported_records < self.records_to_export_count:
            sys.stdout.write(progress_info)
            sys.stdout.flush()
            sys.stdout.write('\b' * len(progress_info))
        else:
            sys.stdout.flush()