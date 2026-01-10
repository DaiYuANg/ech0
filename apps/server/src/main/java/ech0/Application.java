package ech0;

import ech0.instance.DI;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Application {
  void main() {
    DI.INSTANCE.get(Ech0.class).start();
  }
}


