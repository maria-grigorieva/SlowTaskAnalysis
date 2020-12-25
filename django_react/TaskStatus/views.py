from django.shortcuts import render


def task_index(request):
    """
    Renders a page asking to enter an ID
    """
    return render(request, 'index-task.html')


def task_index_preselected(request, jeditaskid):
    """
    Renders a page with a pre-entered ID, immediately starting the search
    """
    return render(request, 'index-task-preselected.html', {'jeditaskid': jeditaskid})


def duration_index(request):
    """
    Renders a page asking to enter the dates for tasks analysis
    """
    return render(request, 'index-duration.html')

def statuses(request):
    """
    Renders a page asking to enter the dates for tasks analysis
    """
    return render(request, 'statuses.html')
