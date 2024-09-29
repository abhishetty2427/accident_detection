<!doctype html>
<html lang="en">
    <?php
    require_once("cdn.php");
    require_once("remote.php");
    require_once("remote1.php");
    ?>
<head>
	<meta charset="utf-8" />
	<link rel="apple-touch-icon" sizes="76x76" href="assets/img/apple-icon.png">
	<link rel="icon" type="image/png" sizes="96x96" href="assets/img/favicon.png">
	<meta http-equiv="X-UA-Compatible" content="IE=edge,chrome=1" />

	<title>Past Accidents</title>
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

</head>
<body>

<div class="wrapper">
	<div class="sidebar" data-background-color="white" data-active-color="danger">

		<div class="sidebar-wrapper">
            <div class="logo">
                <a class="simple-text">
                    ACCIDENT DETECTION
                </a>
            </div>

            <ul class="nav">
                <li>
                    <a href="dashboard.php">
                        <i class="ti-panel"></i>
                        <p>Dashboard</p>
                    </a>
                </li>
                <li>
                    <a href="user.php">
                        <i class="ti-user"></i>
                        <p>Monitoring Region</p>
                    </a>
                </li>
                <li>
                    <a href="eServices.php">
                        <i class="ti-announcement"></i>
                        <p>Emergency Services</p>
                    </a>
                </li>
                <li>
                    <a href="detectAccident.php">
                        <i class="ti-search"></i>
                        <p>Detect Accident</p>
                    </a>
                </li>
                <li class="active">
                    <a href="pastAccidents.php">
                        <i class="ti-bell"></i>
                        <p>Past Accidents</p>
                    </a>
                </li>
                <li>
                    <a href="accidentVideos.php">
                        <i class="ti-text"></i>
                        <p>Test on Videos</p>
                    </a>
                </li>
                <li class="active-pro">
                    <a href="logout.php">
                        <i class="ti-export"></i>
                        <p>Logout</p>
                    </a>
                </li>
            </ul>
    	</div>
    </div>

    <div class="main-panel">
		<nav class="navbar navbar-default">
            <div class="container-fluid">
                <div class="navbar-header">
                    <button type="button" class="navbar-toggle">
                        <span class="sr-only">Toggle navigation</span>
                        <span class="icon-bar bar1"></span>
                        <span class="icon-bar bar2"></span>
                        <span class="icon-bar bar3"></span>
                    </button>
                    <a class="navbar-brand" href="#">Past Detected Accidents</a>
                </div>
                
            </div>
        </nav>


        <div class="content">
            <div class="container-fluid">
                <div class="row">
                    <div class="col-md-12">
                        <div class="card">
                            <div class="header">
                                <h4 class="title"></h4>
                            </div>
                            <div class="content">
                                <table class="table table-striped"">
                <thead>
                    <tr>
                        <th>Sr. No</th>
                        <th>Location</th>
                        <th>Footage</th>
                        <th>Date and Time</th>
                    </tr>
                </thead>
                <tbody>
                    <?php
                        $stat= $conn->prepare('SELECT * FROM `Accident` ORDER BY `timestampAcc`');
                        $stat->execute();
                        $count=1;
                        $stat->bind_result($Accident_id,$camera_id, $pathimg, $date_time,$timestamp);
                        while($stat->fetch()){
                            $stat1 = $conn1->prepare('SELECT `location` FROM `CCTV` WHERE `Camera_id` = ?');
                            $stat1->bind_param('s', $camera_id );
                            $stat1->execute();
                            $stat1->bind_result($location);
                            $stat1->fetch();
                            $stat1->close(); 
                            echo "
                                <tr>
                                <td>".$count."</td>
                                <td>".$location."</td>
                                
                                
                                <td>
                                    <a class='btn btn-primary'href='".$pathimg."'>
                                        View Image
                                    </a>
                            
                                
                                    
                                
                                </td>
                                <td>".$date_time."</td>
                                </tr>
                                ";
                            $count++;
                        }

                    ?>

                </tbody>
            </table>


                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
                                <?php require_once("footer.php"); ?>

            </div>
        </div>


</body>

    <!--   Core JS Files   -->
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
