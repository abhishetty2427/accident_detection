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

	<title>Detect Accident</title>

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
                <li class="active">
                    <a href="detectAccident.php">
                        <i class="ti-search"></i>
                        <p>Detect Accident</p>
                    </a>
                </li>
                <li>
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
                    <a class="navbar-brand" href="#">Detect Accident</a>
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
                                <div class="container-fluid">
                                    <div class="row">
                                        <div class="col-md-12">


                <?php
                $stat= $conn->prepare('SELECT `flag_var` FROM `Flag` limit 1;');
                $stat->execute();
                $stat->bind_result($flag_var);
                $stat->fetch();
                $stat->close();
                $stat= $conn->prepare('SELECT * FROM `Accident` ORDER BY `timestampAcc` DESC limit 1;');
                $stat->execute();
                $stat->bind_result($Accident_id,$camera_id, $pathimg, $date_time,$timesatmp);
                $stat->fetch();
                $stat->close();
                //$stat= $conn->prepare('SELECT `location` FROM `cctv` where `Camera_id`=?');
                //$stat->bind_param("s",$camera_id);
                //$stat->bind_result($location);
                //$stat->fetch();
                //$stat->close();
                if($flag_var==1){
                    echo '
                        
                              <div class="row">
                                <dir class="col-sm-9">
                                    <img style="width: 100%;" src="'.$pathimg.'"> 
                                </dir>
                                <dir class="col-sm-3">
                                    <button class="btn btn-info">'.$date_time.'</button>
                                    <br><br>
                                    <button class="btn btn-info">'.$camera_id.'</button></button>
                                    <br><br>
                                    <button class="btn btn-danger">Alert sent to Emergency Services</button>
                                </dir>
                                
                              </div>';
                    $message = "accident has happed at Nerul Navi Mumbai on ".$date_time." and detected by camera ".$camera_id;
                    //require_once("way2sms.php");
                
                }
                else {
                    echo "<a  class='btn btn-lg btn-danger' style='width: 80%;'>
                    No accident has happened <a href='detectAccident.php' class='btn btn-primary btn-lg' style='float: right'>REFRESH</a><h3>
                     ";
                    
                }
                $stat = $conn->prepare('UPDATE `Flag` SET `flag_var`= 0 WHERE `flag_key` = 1;');
                $stat->execute();
                $stat->close();
        ?>


                                  
                        </div>
                    </div>

                </div>
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
