"""django_react URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include, re_path
from django.conf.urls import url
from TaskStatus import views
from TaskStatus.database import database

urlpatterns = [
    path('admin/', admin.site.urls),

    # Task and job statuses description
    path('statuses/', views.statuses),

    # Single task analysis
    path('task/', views.task_index),
    path('task/<int:jeditaskid>/', views.task_index_preselected),
    url('ajax/request_db', database.request_db, name='request_db'),

    # Duration analysis
    path('', views.duration_index),
    path('duration/', views.duration_index)
]
