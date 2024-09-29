<?php require_once("cdn.php");
require_once("remote.php");
if(isset($_POST['login']))
{
	echo "<H1>success</H1>";
	$user	=$_POST['userID'];
	$pwd	=$_POST['pwd'];
	$stat= $conn->prepare("SELECT `Password` , `Region_id` FROM `Monitoring_Region` WHERE `Region_id`=?");
	$stat->bind_param("s",$user);
	$stat->execute();
	$stat->bind_result($cpwd , $cuser);
	$stat->fetch();
	
	if(($user!=$cuser)){
		echo '<center><div class="alert alert-lg alert-success"><strong>Not register yet!</strong> <a href="login.php"> click here to try again.</a></div></center>';
		
	}
	else if(($pwd==$cpwd)&&($user==$cuser)){
		session_start();
		$_SESSION["user"]=$user;	
		header("location:dashboard.php");
		
	}
	else{
		
		echo '<center><div class="alert alert-lg alert-success"><strong>password incorrect!</strong> <a href="login.php"> click here to try again.</a></div></center>';
	}
	
}
if(isset($_POST['addVidtOBuffer']))
{
	$path = $_SESSION['video'];
	$stat= $conn->prepare('INSERT INTO `vidbuffer`(`path`) VALUES (?)');
    $stat->bind_param('s', $path);
    $stat->execute();
    $stat->close();
    header("location:checkingForAccident.php");
}
?>
