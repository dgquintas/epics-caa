$(document).ready(function() {

    $(".collapse").collapse()

    // https://github.com/eternicode/bootstrap-datepicker
    var dp_from = $('#from_date.datepicker')
    dp_from.datepicker({
        format: 'yyyy/mm/dd',
        autoclose: true,
        endDate: '+0s',
    });
    var dp_to = $('#to_date.datepicker')
    dp_to.datepicker({
        format: 'yyyy/mm/dd',
        autoclose: true,
        endDate: '+0s',
    });
    dp_from.on('changeDate', function(ev){
        from_date = new Date(ev.date);
        dp_to.datepicker('setStartDate', ev.date);
    });

    dp_to.on('changeDate', function(ev){
        to_date = new Date(ev.date);
        dp_from.datepicker('setEndDate', ev.date);
    });


//////////////////////////////

    $('.select-all').click( function(){
        var container = $(this).parents('div#checkboxes');
        container.find('[type=checkbox]').prop("checked", true);
    });
    $('.deselect-all').click( function(){
        var container = $(this).parents('div#checkboxes');
        container.find('[type=checkbox]').prop("checked", false);
    });

////////////////////////////
    
    var oTable = $('#valuestable').dataTable({
        "sDom": 'ftTi',
        "bPaginate": false,
        "aaSorting": [[0, 'desc']],
        "aoColumnDefs": [
            { "aTargets": [ "timestamp_col" ], 
                 "sType": "numeric", 
                 "sClass": "timestamp_td", 
                 "sWidth": "120px" },
            { "aTargets": [ "archived_at_col" ], 
                 "sType": "date", 
                 "sClass": "archived_at_td", 
                 "sWidth": "200px" }
        ], 
        "oTableTools": {
			"sSwfPath": "/static/swf/copy_cvs_xls_pdf.swf", 
            "aButtons": ['copy', 'csv', 'xls', 'pdf']
        }
    });


    $('.timestamp_td').popover({
        content: function(){ 
            return $.timeago($(this).attr("isodate"));
        },
        placement: 'left',
    });
    $(".timeago").timeago();

    $('#values-seemore').tooltip({
        placement: 'top',
        trigger: 'manual',
        title: "Already showing the maximum number of elements (don't be greedy!)",
    });
    $('#values-seemore').click( function() {
        var url = window.location.href;
        var o = $.deparam.querystring(url, true);
        var l;
        if (typeof o.limit === "undefined"){
            l = 20;
        } else{
            l = o.limit + 10;
        }
        if( l <= 100 ){
            window.location = $.param.querystring(url, 'limit='+l);
        }
        else{
            $(this).tooltip('show');
            var that = $(this);
            window.setTimeout( function(){ 
                that.tooltip('hide');
            },
            2000);
        }
    });

    $('#values-seeless').tooltip({
        placement: 'top',
        trigger: 'manual',
        title: 'Already showing the minimum number of elements',
    });
    $('#values-seeless').click( function() {
        var url = window.location.href;
        var o = $.deparam.querystring(url, true);
        var l;
        if (typeof o.limit === "number"){
            l = o.limit - 10;
            if(l >= 10){
                window.location = $.param.querystring(url, 'limit='+l);
            }
            else {
                $(this).tooltip('show');
                var that = $(this);
                window.setTimeout( function(){ 
                    that.tooltip('hide');
                },
                2000);
            }
        } 
    });


    ////////////////////////
    $('select#limit-select')
    $('select#limit-select').change( function() {
        var limit = $('option:selected', this).text();
        window.location = $.param.querystring(window.location.href, 'limit='+limit);
    } );




} );

