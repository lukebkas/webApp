import random

from django.db import IntegrityError
from django.db import connection
from django.http import JsonResponse
from django.shortcuts import render


def song_list(request):
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM songs LIMIT 1")
        all_columns = [col[0] for col in cursor.description]

    available_columns = [col for col in all_columns if col != "index" and col != "track_id"]
    all_columns_ordered = available_columns + (["track_id"] if "track_id" in all_columns else [])

    requested_columns = request.GET.getlist("columns")
    if requested_columns:
        selected_columns = [col for col in requested_columns if col in all_columns_ordered and col != "track_id"]
        if "track_id" in requested_columns and "track_id" in all_columns:
            selected_columns.append("track_id")
        if not selected_columns:
            selected_columns = available_columns
    else:
        selected_columns = available_columns
    if "track_id" in selected_columns:
        selected_columns = [col for col in selected_columns if col != "track_id"] + ["track_id"]

    filter_rows = parse_filters(request, available_columns)
    where_clause, where_params = build_filter_clause(filter_rows)
    where_sql = f" WHERE {where_clause}" if where_clause else ""

    sort_by = request.GET.get("sort_by", "default")
    sort_dir = request.GET.get("sort_dir", "asc").lower()
    if sort_dir not in {"asc", "desc"}:
        sort_dir = "asc"

    sort_column = "`index`" if sort_by == "default" else f"`{sort_by}`"
    if sort_by != "default" and sort_by not in available_columns:
        sort_column = "`index`"
        sort_by = "default"

    order_clause = f"{sort_column} {sort_dir.upper()}"
    columns_sql = ", ".join(f"`{col}`" for col in selected_columns)

    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT {columns_sql} FROM songs{where_sql} ORDER BY {order_clause} LIMIT 500",
            where_params,
        )
        columns = [col[0] for col in cursor.description]
        rows = [list(row) for row in cursor.fetchall()]

    if "explicit" in columns:
        explicit_index = columns.index("explicit")
        for row in rows:
            if explicit_index < len(row):
                if row[explicit_index] == 0:
                    row[explicit_index] = "0 (False)"
                elif row[explicit_index] == 1:
                    row[explicit_index] = "1 (True)"

    if request.headers.get("x-requested-with") == "XMLHttpRequest":
        return JsonResponse({
            "columns": columns,
            "rows": rows,
        })

    return render(
        request,
        "myApp/songs.html",
        {
            "all_columns": all_columns_ordered,
            "columns": columns,
            "rows": rows,
            "selected_columns": selected_columns,
            "selected_sort": sort_by,
            "selected_sort_dir": sort_dir,
            "filter_rows": filter_rows,
        },
    )


def parse_filters(request, available_columns):
    indices = sorted({
        int(key.split("_")[-1])
        for key in request.GET.keys()
        if key.startswith("filter_col_") and key.split("_")[-1].isdigit()
    })
    filters = []
    for idx in indices:
        column = request.GET.get(f"filter_col_{idx}", "").strip()
        op = request.GET.get(f"filter_op_{idx}", "equals")
        val = request.GET.get(f"filter_val_{idx}", "").strip()
        val2 = request.GET.get(f"filter_val2_{idx}", "").strip()
        if not column or column not in available_columns:
            continue
        if op == "between" and not val2:
            continue
        if op != "between" and not val:
            continue
        filters.append({"column": column, "op": op, "val": val, "val2": val2})
    return filters


def build_filter_clause(filters):
    clauses = []
    params = []
    for f in filters:
        col = f["column"]
        op = f["op"]
        val = f["val"]
        val2 = f["val2"]
        quoted = f"`{col}`"

        if op == "equals":
            clauses.append(f"{quoted} = %s")
            params.append(val)
        elif op == "not_equals":
            clauses.append(f"{quoted} != %s")
            params.append(val)
        elif op == "contains":
            clauses.append(f"{quoted} LIKE %s")
            params.append(f"%{val}%")
        elif op == "starts_with":
            clauses.append(f"{quoted} LIKE %s")
            params.append(f"{val}%")
        elif op == "ends_with":
            clauses.append(f"{quoted} LIKE %s")
            params.append(f"%{val}")
        elif op == "lt":
            clauses.append(f"{quoted} < %s")
            params.append(val)
        elif op == "lte":
            clauses.append(f"{quoted} <= %s")
            params.append(val)
        elif op == "gt":
            clauses.append(f"{quoted} > %s")
            params.append(val)
        elif op == "gte":
            clauses.append(f"{quoted} >= %s")
            params.append(val)
        elif op == "between":
            clauses.append(f"{quoted} BETWEEN %s AND %s")
            params.extend([val, val2])
        else:
            clauses.append(f"{quoted} = %s")
            params.append(val)

    return " AND ".join(clauses), params


def generate_track_id():
    return "useradded-" + "".join(random.choices("0123456789", k=11))


def add_song(request):
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM songs LIMIT 1")
        all_columns = [col[0] for col in cursor.description]

    visible_columns = [col for col in all_columns if col not in {"index", "track_id"}]
    initial_values = [(col, request.POST.get(col, "")) for col in visible_columns]
    error = None
    success = False

    if request.method == "POST":
        field_values = [request.POST.get(col, "").strip() or None for col in visible_columns]
        insert_columns = visible_columns + ["track_id"]
        placeholders = ", ".join("%s" for _ in insert_columns)
        columns_sql = ", ".join(f"`{col}`" for col in insert_columns)

        done = 0
        while done == 0:
            track_id = generate_track_id()
            values = field_values + [track_id]
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"INSERT INTO songs ({columns_sql}) VALUES ({placeholders})",
                        values,
                    )
                success = True
                done=1
                break
            except IntegrityError as exc:
                if "Duplicate entry" in str(exc) and "track_id" in str(exc):
                    done = 0
                    continue
                error = str(exc)
                break
            except Exception as exc:
                error = str(exc)
                break

    return render(
        request,
        "myApp/add_song.html",
        {
            "columns": visible_columns,
            "initial_values": initial_values,
            "error": error,
            "success": success,
        },
    )


def delete_song(request):
    error = None
    success = False
    song = None
    columns = []
    track_id = request.POST.get("track_id", "").strip() if request.method == "POST" else ""

    if request.method == "POST" and track_id:
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM songs WHERE track_id = %s LIMIT 1", [track_id])
            row = cursor.fetchone()
            columns = [col[0] for col in cursor.description] if cursor.description else []

        if not row:
            error = f"No song found for track_id '{track_id}'."
        elif request.POST.get("confirm") == "yes":
            try:
                with connection.cursor() as cursor:
                    cursor.execute("DELETE FROM songs WHERE track_id = %s", [track_id])
                success = True
            except Exception as exc:
                error = str(exc)
        else:
            filtered = [(col, value) for col, value in zip(columns, row) if col != "index"]
            song = filtered
            columns = [col for col in columns if col != "index"]
    elif request.method == "POST":
        error = "Please enter a track_id."

    return render(
        request,
        "myApp/delete_song.html",
        {
            "track_id": track_id,
            "error": error,
            "success": success,
            "song": song,
            "columns": columns,
        },
    )


def edit_song(request):
    error = None
    success = False
    song_fields = []
    track_id = ""

    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM songs LIMIT 1")
        all_columns = [col[0] for col in cursor.description]

    editable_columns = [col for col in all_columns if col not in {"index", "track_id"}]

    if request.method == "POST":
        track_id = request.POST.get("track_id", "").strip()
        if not track_id:
            error = "Please enter a track_id."
        elif request.POST.get("update") == "yes":
            field_values = [request.POST.get(col, "").strip() or None for col in editable_columns]
            set_clause = ", ".join(f"`{col}` = %s" for col in editable_columns)
            values = field_values + [track_id]
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"UPDATE songs SET {set_clause} WHERE track_id = %s",
                        values,
                    )
                if cursor.rowcount == 0:
                    error = f"No song found for track_id '{track_id}'."
                else:
                    success = True
            except Exception as exc:
                error = str(exc)
        else:
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM songs WHERE track_id = %s LIMIT 1", [track_id])
                row = cursor.fetchone()
                columns = [col[0] for col in cursor.description] if cursor.description else []
            if not row:
                error = f"No song found for track_id '{track_id}'."
            else:
                song_fields = [
                    (col, value) for col, value in zip(columns, row) if col in editable_columns
                ]
    return render(
        request,
        "myApp/edit_song.html",
        {
            "track_id": track_id,
            "error": error,
            "success": success,
            "song_fields": song_fields,
            "editable_columns": editable_columns,
        },
    )


def delete_current_view(request):
    error = None
    success = False
    deleted_count = 0
    row_count = 0
    selected_columns = []

    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM songs LIMIT 1")
        all_columns = [col[0] for col in cursor.description]

    available_columns = [col for col in all_columns if col != "index"]
    selected_columns = request.GET.getlist("columns") or available_columns
    selected_columns = [col for col in selected_columns if col in available_columns]
    if not selected_columns:
        selected_columns = available_columns

    sort_by = request.GET.get("sort_by", "default")
    sort_dir = request.GET.get("sort_dir", "asc").lower()
    if sort_dir not in {"asc", "desc"}:
        sort_dir = "asc"

    sort_column = "`index`" if sort_by == "default" else f"`{sort_by}`"
    if sort_by != "default" and sort_by not in available_columns:
        sort_column = "`index`"
        sort_by = "default"

    order_clause = f"{sort_column} {sort_dir.upper()}"

    filter_rows = parse_filters(request, available_columns)
    where_clause, where_params = build_filter_clause(filter_rows)
    where_sql = f" WHERE {where_clause}" if where_clause else ""

    with connection.cursor() as cursor:
        cursor.execute(f"SELECT track_id FROM songs{where_sql} ORDER BY {order_clause} LIMIT 500", where_params)
        track_rows = [row[0] for row in cursor.fetchall()]
        row_count = len(track_rows)

    if request.method == "POST" and request.POST.get("confirm") == "yes":
        if row_count > 0:
            placeholders = ", ".join("%s" for _ in track_rows)
            with connection.cursor() as cursor:
                cursor.execute(
                    f"DELETE FROM songs WHERE track_id IN ({placeholders})",
                    track_rows,
                )
                deleted_count = cursor.rowcount
        success = True

    return render(
        request,
        "myApp/delete_view.html",
        {
            "row_count": row_count,
            "success": success,
            "deleted_count": deleted_count,
            "error": error,
            "query_string": request.META.get("QUERY_STRING", ""),
        },
    )


def report_current_view(request):
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM songs LIMIT 1")
        all_columns = [col[0] for col in cursor.description]

    available_columns = [col for col in all_columns if col != "index"]
    selected_columns = request.GET.getlist("columns") or available_columns
    selected_columns = [col for col in selected_columns if col in available_columns]
    if not selected_columns:
        selected_columns = available_columns

    filter_rows = parse_filters(request, available_columns)
    where_clause, where_params = build_filter_clause(filter_rows)
    where_sql = f" WHERE {where_clause}" if where_clause else ""

    numeric_types = {
        "int",
        "smallint",
        "mediumint",
        "bigint",
        "decimal",
        "double",
        "float",
        "numeric",
        "tinyint",
        "year",
    }

    type_map = {}
    if selected_columns:
        placeholders = ", ".join("%s" for _ in selected_columns)
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = DATABASE() AND table_name = 'songs' "
                f"AND column_name IN ({placeholders})",
                selected_columns,
            )
            type_map = {row[0]: row[1].lower() for row in cursor.fetchall()}

    numeric_columns = [col for col in selected_columns if type_map.get(col, "") in numeric_types]
    string_columns = [col for col in selected_columns if col != "track_id" and col not in numeric_columns]

    # Count number of rows matching filters
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM songs{where_sql}", where_params)
        selected_row_count = cursor.fetchone()[0]

    numeric_stats = []
    with connection.cursor() as cursor:
        for col in numeric_columns:
            cursor.execute(
                f"SELECT AVG(`{col}`), MAX(`{col}`), MIN(`{col}`) FROM songs{where_sql}",
                where_params,
            )
            avg_val, max_val, min_val = cursor.fetchone()
            numeric_stats.append(
                {
                    "column": col,
                    "average": avg_val,
                    "max": max_val,
                    "min": min_val,
                }
            )

    string_modes = []
    with connection.cursor() as cursor:
        for col in string_columns:
            cursor.execute(
                f"SELECT `{col}`, COUNT(*) FROM songs{where_sql} GROUP BY `{col}` "
                f"ORDER BY COUNT(*) DESC, `{col}` ASC",
                where_params,
            )
            counts = cursor.fetchall()
            if not counts:
                continue

            top_count = counts[0][1]
            if top_count <= 2:
                continue

            tied = [value for value, count in counts if count == top_count]
            if len(tied) > 3:
                continue

            string_modes.append(
                {
                    "column": col,
                    "most_common": ", ".join(str(value) for value in tied),
                    "count": top_count,
                    "is_tie": len(tied) > 1,
                    "tie_count": len(tied),
                }
            )

    return render(
        request,
        "myApp/report_view.html",
        {
            "numeric_stats": numeric_stats,
            "string_modes": string_modes,
            "selected_row_count": selected_row_count,
            "query_string": request.META.get("QUERY_STRING", ""),
        },
    )