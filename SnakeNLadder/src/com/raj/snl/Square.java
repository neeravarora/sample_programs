package com.raj.snl;

public class Square {
	private Board board = null;
	private Player player = null;
	private int position;
	private SquareRole squareRole = null;

	public Square(int pos, Board b) {
		assert pos >= 0 : " Square number must be positive or zero ";
		position = pos;
		board = b;
	}

	public Player getPlayer() {
		return player;
	}

	public void setPlayer(Player p) {
		player = p;
	}

	public int getPosition() {
		return position;
	}

	public void setSquareRole(SquareRole sr) {
		assert sr != null;
		squareRole = sr;
	}

	public boolean isOccupied() {
		return squareRole.isOccupied();
	}

	public boolean isLastSquare() {
		return squareRole.isLastSquare();
	}

	public Square moveAndLand(int moves) {
		return squareRole.moveAndLand(moves);
	}

	public Square landHereOrGoHome() {
		return squareRole.landHereOrGoHome();
	}

	public void enter(Player p) {
		squareRole.enter(p);
	}

	public void leave(Player p) {
		squareRole.leave(p);
	}

	public Square findRelativeSquare(int shift) {
		return board.findSquare(position + shift);
	}

	public Square findFirstSquare() {
		return board.firstSquare();
	}

	public Square findLastSquare() {
		return board.lastSquare();
	}
}