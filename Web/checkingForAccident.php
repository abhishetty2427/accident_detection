<!DOCTYPE html>
<html >
<head>
	<meta charset="utf-8" />
	<link rel="apple-touch-icon" sizes="76x76" href="assets/img/apple-icon.png">
	<link rel="icon" type="image/png" sizes="96x96" href="assets/img/favicon.png">
	<meta http-equiv="X-UA-Compatible" content="IE=edge,chrome=1" />
    <?php require_once("cdn.php"); ?>

	<title>Checking for Accident</title>

	<meta content='width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=0' name='viewport' />
    <meta name="viewport" content="width=device-width" />


    <!-- Bootstrap core CSS     -->
    <link href="assets/css/bootstrap.min.css" rel="stylesheet" />

    <!-- Animation library for notifications   -->
    <link href="assets/css/animate.min.css" rel="stylesheet"/>

    <!--  Paper Dashboard core CSS    -->
    <link href="assets/css/paper-dashboard.css" rel="stylesheet"/>

    <!--  CSS for Demo Purpose, don't include it in your project     -->
    <link href="assets/css/demo.css" rel="stylesheet" />

    <!--  Fonts and icons     -->
    <link href="https://maxcdn.bootstrapcdn.com/font-awesome/latest/css/font-awesome.min.css" rel="stylesheet">
    <link href='https://fonts.googleapis.com/css?family=Muli:400,300' rel='stylesheet' type='text/css'>
    <link href="assets/css/themify-icons.css" rel="stylesheet">
    <style type="text/css">
    </style>

</head>
<body>

				<div class="container-fluid">
					<nav class="navbar navbar-default">
			            <div class="container-fluid">
			                <div class="navbar-header">
			                    <a class="navbar-brand" href="#">Checking for accident</a>
			                </div>
			                
			            </div>
			        </nav>
				</div>

                <div class="row">
	                
                	<div class="col-sm-3"></div>
                    <div class="col-sm-6">
	                    	<div class="bs-example">
					    		<div class="progress">
					        		<div class="progress-bar progress-bar-striped" id="bar">
					        		</div>
					    		</div>
					    	</div>
                            <div class="container" >
                            <?php
                            
                            echo "
                            <video control='control' autoplay loop style='width: 56%'  src='".$_SESSION['video']."'  type='mp4/video '>
                            </video>
                            ";
                            ob_start();
							header( "refresh:120;url=detectAccident.php" );
                            ?>
                            </div>
                    </div>
                	<div class="col-sm-3"></div>
                </div>
            </div>



    </div>
</div>

</body>
    <!--   Core JS Files   -->
    <script type="text/javascript">
        var i = 0;
        var progressBar = $("#bar");
        function countNumbers(){
            if(i < 100){
                i = i + 1;
                progressBar.css("width", i + "%");
            }
            // Wait for sometime before running this script again
            setTimeout("countNumbers()", 1000);
        }
        countNumbers();
    </script>
    <script src="assets/js/jquery.min.js" type="text/javascript"></script>
	<script src="assets/js/bootstrap.min.js" type="text/javascript"></script>

	<!--  Checkbox, Radio & Switch Plugins -->
	<script src="assets/js/bootstrap-checkbox-radio.js"></script>

	<!--  Charts Plugin -->
	<script src="assets/js/chartist.min.js"></script>

    <!--  Notifications Plugin    -->
    <script src="assets/js/bootstrap-notify.js"></script>

    <!--  Google Maps Plugin    -->
    <script type="text/javascript" src="https://maps.googleapis.com/maps/api/js"></script>

    <!-- Paper Dashboard Core javascript and methods for Demo purpose -->
	<script src="assets/js/paper-dashboard.js"></script>

	<!-- Paper Dashboard DEMO methods, don't include it in your project! -->
	<script src="assets/js/demo.js"></script>

</html>
