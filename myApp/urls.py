from django.urls import path
from . import views

urlpatterns = [
    path("", views.song_list, name="song_list"),
    path("songs/", views.song_list, name="songs"),
    path("add/", views.add_song, name="add_song"),
    path("delete/", views.delete_song, name="delete_song"),
    path("edit/", views.edit_song, name="edit_song"),
    path("delete_view/", views.delete_current_view, name="delete_current_view"),
    path("report_view/", views.report_current_view, name="report_current_view"),
]